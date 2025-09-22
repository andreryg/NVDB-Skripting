from shapely import wkt, LineString, Polygon, distance, Point, geometry

class LineStringToPolygonConverter:
    def __init__(self, linestring: geometry.base.BaseGeometry, ring_threshold: float, validate: bool = True):
        if not isinstance(linestring, LineString):
            raise TypeError("Input must be a LineString geometry.")
        self.linestring = linestring
        self.ring_threshold = ring_threshold
        self.validate = validate
        
        # If the LineString is not closed, create a new closed LineString instance.
        # Note: This class will use the closed LineString internally, which may differ from the original object.
        if self.linestring.coords[0] != self.linestring.coords[-1]:
            if distance(Point(self.linestring.coords[0]), Point(self.linestring.coords[-1])) > self.ring_threshold:
                raise ValueError("The LineString can not be closed, the endpoints are too far apart.")
            coords = list(self.linestring.coords)
            coords.append(coords[0])
            closed_linestring = LineString(coords)
            self.linestring = closed_linestring

        if self.validate:
            if not self.num_points_validation():
                raise ValueError("The LineString must have at least 4 points to form a valid Polygon.")
            if self.self_intersection_validation():
                raise ValueError("The LineString is self-intersecting and cannot be converted to a valid Polygon.")
            
        self.polygon = self.convert()

    def __repr__(self):
        return f"LineStringToPolygonConverter(linestring={self.linestring.wkt}, ring_threshold={self.ring_threshold}, validate={self.validate})"

    def num_points_validation(self) -> bool:
        # A valid Polygon requires at least 4 points (including the closing point).
        return len(self.linestring.coords) >= 4
    
    def self_intersection_validation(self) -> bool:
        # A LineString should not be self-intersecting to form a valid Polygon.
        return not self.linestring.is_simple

    def convert(self) -> Polygon:
        coords = list(self.linestring.coords)
        return Polygon(coords)
    
    def get_polygon(self) -> Polygon:
        return self.polygon

if __name__ == "__main__":
    linje = "LINESTRING Z (56763.102 6457032.938 4.86,56763.365 6457032.507 4.97,56763.49 6457032.224 5.01,56763.661 6457031.877 4.97,56763.846 6457031.478 4.87,56768.383 6457033.599 4.89,56768.273 6457033.951 4.98,56768.088 6457034.35 5,56767.901 6457034.739 4.95,56767.693 6457035.119 4.83,56763.102 6457032.938 4.86)"
    ls = wkt.loads(linje)
    print(LineStringToPolygonConverter(ls, ring_threshold=1.0).get_polygon())