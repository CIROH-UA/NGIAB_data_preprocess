CREATE TRIGGER 'gpkg_tile_matrix_zoom_level_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: zoom_level cannot be less than 0') WHERE (NEW.zoom_level < 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_zoom_level_update' BEFORE UPDATE of zoom_level ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: zoom_level cannot be less than 0') WHERE (NEW.zoom_level < 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_width_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: matrix_width cannot be less than 1') WHERE (NEW.matrix_width < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_width_update' BEFORE UPDATE OF matrix_width ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: matrix_width cannot be less than 1') WHERE (NEW.matrix_width < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_height_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: matrix_height cannot be less than 1') WHERE (NEW.matrix_height < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_matrix_height_update' BEFORE UPDATE OF matrix_height ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: matrix_height cannot be less than 1') WHERE (NEW.matrix_height < 1); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_x_size_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: pixel_x_size must be greater than 0') WHERE NOT (NEW.pixel_x_size > 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_x_size_update' BEFORE UPDATE OF pixel_x_size ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: pixel_x_size must be greater than 0') WHERE NOT (NEW.pixel_x_size > 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_y_size_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: pixel_y_size must be greater than 0') WHERE NOT (NEW.pixel_y_size > 0); END;
CREATE TRIGGER 'gpkg_tile_matrix_pixel_y_size_update' BEFORE UPDATE OF pixel_y_size ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: pixel_y_size must be greater than 0') WHERE NOT (NEW.pixel_y_size > 0); END;
CREATE TRIGGER "rtree_flowpaths_geom_insert" AFTER INSERT ON "flowpaths" WHEN (new."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_flowpaths_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_flowpaths_geom_update1" AFTER UPDATE OF "geom" ON "flowpaths" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_flowpaths_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_flowpaths_geom_update2" AFTER UPDATE OF "geom" ON "flowpaths" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_flowpaths_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "rtree_flowpaths_geom_update3" AFTER UPDATE ON "flowpaths" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_flowpaths_geom" WHERE id = OLD."fid"; INSERT OR REPLACE INTO "rtree_flowpaths_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_flowpaths_geom_update4" AFTER UPDATE ON "flowpaths" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_flowpaths_geom" WHERE id IN (OLD."fid", NEW."fid"); END;
CREATE TRIGGER "rtree_flowpaths_geom_delete" AFTER DELETE ON "flowpaths" WHEN old."geom" NOT NULL BEGIN DELETE FROM "rtree_flowpaths_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "trigger_insert_feature_count_flowpaths" AFTER INSERT ON "flowpaths" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('flowpaths'); END;
CREATE TRIGGER "trigger_delete_feature_count_flowpaths" AFTER DELETE ON "flowpaths" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('flowpaths'); END;
CREATE TRIGGER "rtree_divides_geom_insert" AFTER INSERT ON "divides" WHEN (new."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_divides_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_divides_geom_update1" AFTER UPDATE OF "geom" ON "divides" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_divides_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_divides_geom_update2" AFTER UPDATE OF "geom" ON "divides" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_divides_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "rtree_divides_geom_update3" AFTER UPDATE ON "divides" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_divides_geom" WHERE id = OLD."fid"; INSERT OR REPLACE INTO "rtree_divides_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_divides_geom_update4" AFTER UPDATE ON "divides" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_divides_geom" WHERE id IN (OLD."fid", NEW."fid"); END;
CREATE TRIGGER "rtree_divides_geom_delete" AFTER DELETE ON "divides" WHEN old."geom" NOT NULL BEGIN DELETE FROM "rtree_divides_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "trigger_insert_feature_count_divides" AFTER INSERT ON "divides" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('divides'); END;
CREATE TRIGGER "trigger_delete_feature_count_divides" AFTER DELETE ON "divides" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('divides'); END;
CREATE TRIGGER "rtree_lakes_geom_insert" AFTER INSERT ON "lakes" WHEN (new."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_lakes_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_lakes_geom_update1" AFTER UPDATE OF "geom" ON "lakes" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_lakes_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_lakes_geom_update2" AFTER UPDATE OF "geom" ON "lakes" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_lakes_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "rtree_lakes_geom_update3" AFTER UPDATE ON "lakes" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_lakes_geom" WHERE id = OLD."fid"; INSERT OR REPLACE INTO "rtree_lakes_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_lakes_geom_update4" AFTER UPDATE ON "lakes" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_lakes_geom" WHERE id IN (OLD."fid", NEW."fid"); END;
CREATE TRIGGER "rtree_lakes_geom_delete" AFTER DELETE ON "lakes" WHEN old."geom" NOT NULL BEGIN DELETE FROM "rtree_lakes_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "trigger_insert_feature_count_lakes" AFTER INSERT ON "lakes" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('lakes'); END;
CREATE TRIGGER "trigger_delete_feature_count_lakes" AFTER DELETE ON "lakes" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('lakes'); END;
CREATE TRIGGER "trigger_insert_feature_count_pois" AFTER INSERT ON "pois" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('pois'); END;
CREATE TRIGGER "trigger_delete_feature_count_pois" AFTER DELETE ON "pois" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('pois'); END;
CREATE TRIGGER "trigger_insert_feature_count_hydrolocations" AFTER INSERT ON "hydrolocations" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('hydrolocations'); END;
CREATE TRIGGER "trigger_delete_feature_count_hydrolocations" AFTER DELETE ON "hydrolocations" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('hydrolocations'); END;
CREATE TRIGGER "trigger_insert_feature_count_flowpath-attributes" AFTER INSERT ON "flowpath-attributes" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('flowpath-attributes'); END;
CREATE TRIGGER "trigger_delete_feature_count_flowpath-attributes" AFTER DELETE ON "flowpath-attributes" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('flowpath-attributes'); END;
CREATE TRIGGER "trigger_insert_feature_count_flowpath-attributes-ml" AFTER INSERT ON "flowpath-attributes-ml" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('flowpath-attributes-ml'); END;
CREATE TRIGGER "trigger_delete_feature_count_flowpath-attributes-ml" AFTER DELETE ON "flowpath-attributes-ml" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('flowpath-attributes-ml'); END;
CREATE TRIGGER "trigger_insert_feature_count_network" AFTER INSERT ON "network" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('network'); END;
CREATE TRIGGER "trigger_delete_feature_count_network" AFTER DELETE ON "network" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('network'); END;
CREATE TRIGGER "rtree_nexus_geom_insert" AFTER INSERT ON "nexus" WHEN (new."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_nexus_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_nexus_geom_update1" AFTER UPDATE OF "geom" ON "nexus" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN INSERT OR REPLACE INTO "rtree_nexus_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_nexus_geom_update2" AFTER UPDATE OF "geom" ON "nexus" WHEN OLD."fid" = NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_nexus_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "rtree_nexus_geom_update3" AFTER UPDATE ON "nexus" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" NOTNULL AND NOT ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_nexus_geom" WHERE id = OLD."fid"; INSERT OR REPLACE INTO "rtree_nexus_geom" VALUES (NEW."fid",ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"),ST_MinY(NEW."geom"), ST_MaxY(NEW."geom")); END;
CREATE TRIGGER "rtree_nexus_geom_update4" AFTER UPDATE ON "nexus" WHEN OLD."fid" != NEW."fid" AND (NEW."geom" ISNULL OR ST_IsEmpty(NEW."geom")) BEGIN DELETE FROM "rtree_nexus_geom" WHERE id IN (OLD."fid", NEW."fid"); END;
CREATE TRIGGER "rtree_nexus_geom_delete" AFTER DELETE ON "nexus" WHEN old."geom" NOT NULL BEGIN DELETE FROM "rtree_nexus_geom" WHERE id = OLD."fid"; END;
CREATE TRIGGER "trigger_insert_feature_count_nexus" AFTER INSERT ON "nexus" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('nexus'); END;
CREATE TRIGGER "trigger_delete_feature_count_nexus" AFTER DELETE ON "nexus" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('nexus'); END;
CREATE TRIGGER "trigger_insert_feature_count_divide-attributes" AFTER INSERT ON "divide-attributes" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count + 1 WHERE lower(table_name) = lower('divide-attributes'); END;
CREATE TRIGGER "trigger_delete_feature_count_divide-attributes" AFTER DELETE ON "divide-attributes" BEGIN UPDATE gpkg_ogr_contents SET feature_count = feature_count - 1 WHERE lower(table_name) = lower('divide-attributes'); END;
CREATE TRIGGER "rtree_hydrolocations_geom_insert"
AFTER INSERT ON "hydrolocations"
WHEN (new."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom"))
BEGIN
INSERT OR REPLACE INTO "rtree_hydrolocations_geom" VALUES (NEW.ROWID, ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"), ST_MinY(NEW."geom"), ST_MaxY(NEW."geom"));
END;
CREATE TRIGGER "rtree_hydrolocations_geom_update1"
AFTER UPDATE OF "geom" ON "hydrolocations"
WHEN OLD.ROWID = NEW.ROWID AND (NEW."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom"))
BEGIN
INSERT OR REPLACE INTO "rtree_hydrolocations_geom" VALUES (NEW.ROWID, ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"), ST_MinY(NEW."geom"), ST_MaxY(NEW."geom"));
END;
CREATE TRIGGER "rtree_hydrolocations_geom_update2"
AFTER UPDATE OF "geom" ON "hydrolocations"
WHEN OLD.ROWID = NEW.ROWID AND (NEW."geom" IS NULL OR ST_IsEmpty(NEW."geom"))
BEGIN
DELETE FROM "rtree_hydrolocations_geom" WHERE id = OLD.ROWID;
END;
CREATE TRIGGER "rtree_hydrolocations_geom_update3"
AFTER UPDATE OF "geom" ON "hydrolocations"
WHEN OLD.ROWID != NEW.ROWID AND (NEW."geom" NOT NULL AND NOT ST_IsEmpty(NEW."geom"))
BEGIN
DELETE FROM "rtree_hydrolocations_geom" WHERE id = OLD.ROWID;
INSERT OR REPLACE INTO "rtree_hydrolocations_geom" VALUES (NEW.ROWID, ST_MinX(NEW."geom"), ST_MaxX(NEW."geom"), ST_MinY(NEW."geom"), ST_MaxY(NEW."geom"));
END;
CREATE TRIGGER "rtree_hydrolocations_geom_update4"
AFTER UPDATE ON "hydrolocations"
WHEN OLD.ROWID != NEW.ROWID AND (NEW."geom" IS NULL OR ST_IsEmpty(NEW."geom"))
BEGIN
DELETE FROM "rtree_hydrolocations_geom" WHERE id IN (OLD.ROWID, NEW.ROWID);
END;
CREATE TRIGGER "rtree_hydrolocations_geom_delete"
AFTER DELETE ON "hydrolocations"WHEN old."geom" NOT NULL
BEGIN
DELETE FROM "rtree_hydrolocations_geom" WHERE id = OLD.ROWID;
END;
