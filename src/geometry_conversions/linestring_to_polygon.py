from shapely import wkt, LineString, Polygon, distance, Point, geometry

def linestring_to_polygon(linestring: LineString, ring_threshold: float = 1.0) -> Polygon|None:
    """Converts a LineString to a Polygon.

    Converts a shapely LineString to a shapely Polygon by closing the LineString
    The closing is done by adding the start point to the end of the LineString if the distance between them is less than the ring_threshold.
    If the LineString is already closed, it is used as is.
    
    Parameters
    ----------
    linestring : LineString
        The input LineString geometry.
    ring_threshold : float, default=1.0
        The maximum distance between the start and end points to consider the LineString as closable.

    Returns
    -------
    Polygon
        The resulting Polygon geometry.
    None
        If the LineString cannot be closed or converted to a Polygon.
    """
    def linestring_validation(linestring: LineString) -> bool:
        # A valid Polygon requires at least 4 points (including the closing point).
        if len(linestring.coords) < 4:
            return False
        # A LineString should not be self-intersecting to form a valid Polygon.
        if not linestring.is_simple:
            return False
        return True

    coords = list(linestring.coords)
    if linestring.coords[0] != linestring.coords[-1]:
        if distance(Point(linestring.coords[0]), Point(linestring.coords[-1])) > ring_threshold:
            print("The LineString cannot be closed, the endpoints are too far apart.")
            return None # Cannot close the LineString
        coords.append(coords[0])
    closed_linestring = LineString(coords)
    if not linestring_validation(closed_linestring):
        print("The LineString is not valid for conversion to Polygon.")
        return None
    polygon = Polygon(list(closed_linestring.coords))
    return polygon

if __name__ == "__main__":
    linje = "LINESTRING Z (56765.102 6457032.938 4.86,56763.365 6457032.507 4.97,56763.49 6457032.224 5.01,56763.661 6457031.877 4.97,56763.846 6457031.478 4.87,56768.383 6457033.599 4.89,56768.273 6457033.951 4.98,56768.088 6457034.35 5,56767.901 6457034.739 4.95,56767.693 6457035.119 4.83,56765.802 6457032.938 4.86)"
    ls = wkt.loads(linje)
    if not isinstance(ls, LineString):
        raise TypeError("Input must be a LineString geometry.")
    print(linestring_to_polygon(ls, ring_threshold=1.0))