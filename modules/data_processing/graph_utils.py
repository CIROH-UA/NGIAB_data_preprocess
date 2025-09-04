import logging
import sqlite3
from functools import cache
from pathlib import Path
from typing import List, Optional, Set, Union

import igraph as ig
from data_processing.file_paths import FilePaths

logger = logging.getLogger(__name__)


def get_from_to_id_pairs(
    hydrofabric: Path = FilePaths.conus_hydrofabric, ids: Optional[Set | List] = None
) -> List[tuple]:
    """
    Retrieves the from and to IDs from the specified hydrofabric.

    This function reads the from and to IDs from the specified hydrofabric and returns them as a list of tuples.

    Args:
        hydrofabric (Path, optional): The file path to the hydrofabric. Defaults to FilePaths.conus_hydrofabric.
        ids (Set, optional): A set of IDs to filter the results. Defaults to None.
    Returns:
        List[tuple]: A list of tuples containing the from and to IDs.
    """
    sql_query = "SELECT id, toid, divide_id FROM network WHERE id IS NOT NULL"
    if ids:
        ids = [f"'{x}'" for x in ids]
        sql_query = f"{sql_query} AND id IN ({','.join(ids)}) AND toid IN ({','.join(ids)})"
    try:
        con = sqlite3.connect(str(hydrofabric.absolute()))
        edges = con.execute(sql_query).fetchall()
        con.close()
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise
    unique_edges = list(set(edges))
    return unique_edges


def create_graph_from_gpkg(hydrofabric: Path) -> ig.Graph:
    """
    Creates a graph from the specified hydrofabric.

    This function reads the hydrological data from the specified geopackage file and creates a graph from it.

    Args:
        hydrofabric (Path): The file path to the hydrofabric.

    Returns:
        ig.Graph: The hydrological network graph.
    """
    logger.info("Building network graph")
    network = get_from_to_id_pairs(hydrofabric)
    # this is such a mess, if someone knows igraph better, please save me
    # trying to reduce looping and O(n) lookups as much as possible
    vertices = set()
    cats = dict()
    edges = set()
    for id, toid, cat in network:
        vertices.add(id)
        vertices.add(toid)
        edges.add((id, toid))
        cats[id] = cat
    vertices = list(vertices)
    # loop over this once to create an index lookup, built the cat list while we're at it
    vert_dict = dict()
    vert_cats = list()
    for i, v in enumerate(vertices):
        vert_dict[v] = i
        vert_cats.append(cats.get(v, None))

    edge_list = list()

    # build the edge list using the index lookup dict
    for w, n in edges:
        edge = (vert_dict[w], vert_dict[n])
        edge_list.append(edge)

    # edges [(0,1),(0,2)...]
    # vertex_attrs dict = {"name":["wb-121","wb-121"...], "cat":["cat-121","cat-121"...]]
    attrs = {"name": vertices, "cat": vert_cats}
    graph = ig.Graph(edges=edge_list, directed=True, vertex_attrs=attrs)
    return graph


@cache
def get_graph() -> ig.Graph:
    """
    Attempts to load a graph from a pickled file; if unavailable, creates it from the geopackage.

    This function first checks if a pickled version of the graph exists. If not, it creates a new graph
    by reading hydrological data from a geopackage file and then pickles the newly created graph for future use.

    Returns:
        ig.Graph: The hydrological network graph.
    """
    pickled_graph_path = FilePaths.hydrofabric_graph
    if not pickled_graph_path.exists():
        logger.debug("Graph pickle does not exist, creating a new graph.")
        network_graph = create_graph_from_gpkg(FilePaths.conus_hydrofabric)
        network_graph.write_pickle(pickled_graph_path)
    else:
        try:
            network_graph = ig.Graph.Read_Pickle(pickled_graph_path)
        except Exception as e:
            logger.error(f"Error loading graph pickle: {e}")
            raise

    logger.debug(network_graph.summary())
    return network_graph


def get_outlet_id(wb_or_cat_id: str) -> str | None:
    """
    Retrieves the ID of the node downstream of the given node in the hydrological network.

    Given a node name, this function identifies the downstream node in the network, effectively tracing the water flow
    towards the outlet.

    When finding the upstreams of a 'wb' waterbody or 'cat' catchment, what we actually want is the upstreams of the outlet of the 'wb'.

    Args:
        name (str): The name of the node.

    Returns:
        str: The ID of the node downstream of the specified node.
    """
    # all the watebody and catchment IDs are the same, but the graph nodes are named wb-<id>
    # remove everything that isn't a digit, then prepend wb- to get the graph node name
    stem = "".join(filter(str.isdigit, wb_or_cat_id))
    name = f"wb-{stem}"
    graph = get_graph()
    node_index = graph.vs.find(name=name).index
    # this returns the current node, and every node downstream of it in order
    downstream_node = graph.subcomponent(node_index, mode="OUT")
    if len(downstream_node) >= 2:
        # if there is more than one node in the list,
        # then the second is the downstream node of the first
        return graph.vs[downstream_node[1]]["name"]
    return None


def get_upstream_cats(names: Union[str, List[str]]) -> Set[str]:
    """
    Retrieves IDs of all catchments upstream of, and including, the given catchment in the hydrological network.

    Given one or more node names, this function identifies all upstream catchments in the network,
    effectively tracing the water flow back to its source(s).

    Args:
        names (Union[str, List[str]]): A single node name or a list of catchments names.

    Returns:
        Set[str]: A list of IDs for all nodes upstream of the specified node(s). INCLUDING THE INPUT NODES.
    """
    graph = get_graph()
    if isinstance(names, str):
        names = [names]
    # still keeping track of parent ids do we don't read info from overlapping networks more than once
    parent_ids = set()
    cat_ids = set()
    for name in names:
        if name in parent_ids:
            continue
        try:
            if "cat" in name:
                node_index = graph.vs.find(cat=name).index
            else:
                node_index = graph.vs.find(name=name).index
            upstream_nodes = graph.subcomponent(node_index, mode="IN")
            for node in upstream_nodes:
                parent_ids.add(graph.vs[node]["name"])
                cat_ids.add(graph.vs[node]["cat"])
        except KeyError:
            logger.critical(f"Catchment {name} not found in the hydrofabric graph.")
        except ValueError:
            logger.critical(f"Catchment {name} not found in the hydrofabric graph.")
    # sometimes returns None, which isn't helpful
    if None in cat_ids:
        cat_ids.remove(None)

    return cat_ids


def get_upstream_ids(names: Union[str, List[str]], include_outlet: bool = True) -> Set[str]:
    """
    Retrieves IDs of all nodes upstream of, and including, the given nodes in the hydrological network.

    Given one or more node names, this function identifies all upstream nodes in the network,
    effectively tracing the water flow back to its source(s).

    Args:
        names (Union[str, List[str]]): A single node name or a list of node names.

    Returns:
        Set[str]: A list of IDs for all nodes upstream of the specified node(s). INCLUDING THE INPUT NODES.
    """
    graph = get_graph()
    if isinstance(names, str):
        names = [names]
    parent_ids = set()
    for name in names:
        if ("wb" in name or "cat" in name) and include_outlet:
            name = get_outlet_id(name)
        if name in parent_ids:
            continue
        try:
            if "cat" in name:  # type: ignore # If name is None, this will raise an error, which is handled below
                node_index = graph.vs.find(cat=name).index
            else:
                node_index = graph.vs.find(name=name).index
            upstream_nodes = graph.subcomponent(node_index, mode="IN")
            for node in upstream_nodes:
                parent_ids.add(graph.vs[node]["name"])
        except KeyError:
            logger.error(f"feature {name} not found in the hydrofabric graph.")
        except ValueError:
            logger.error(f"feature {name} not found in the hydrofabric graph.")

    return parent_ids
