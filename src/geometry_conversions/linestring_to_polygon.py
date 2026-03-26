from shapely import wkt, LineString, Polygon, distance, Point, geometry, force_2d, has_z

def linestring_to_polygon(linestring: LineString, method : str = 'closed_line', threshold: float = 1.0, z_independence = True) -> Polygon|None:
    """Converts a LineString to a Polygon.

    Converts a Shapely LineString to a Shapely Polygon using the specified method:
    -  closed_line: Closing of line is done by adding the start point to the end of the LineString if the distance between them is less than the threshold. (Default)
    -  buffer: Create a buffer around the LineString to form a Polygon.
    
    Parameters
    ----------
    linestring : LineString
        The input LineString geometry.
    ring_threshold : float, default=1.0
        The maximum distance between the start and end points to consider the LineString as closable.
    z_independence : Boolean, default=True
        Should the z-coordinate be counted when determining if the start and end points are the same.

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

    def closed_line(linestring: LineString) -> Polygon|None:
        coords = list(linestring.coords)
        start_point = coords[0]
        end_point = coords[-1]
        start_z, end_z = 1, 1
        if z_independence and len(start_point) == 3 and len(end_point) == 3: # type: ignore
            start_z, end_z = start_point[2], end_point[2]
            start_point, end_point = start_point[0:2], end_point[0:2]
        if start_point != end_point:
            if distance(Point(start_point), Point(end_point)) > threshold:
                print("The LineString cannot be closed, the endpoints are too far apart.")
                return None # Cannot close the LineString
            elif distance(Point(start_point), Point(end_point)) <= 0.011:
                coords = coords[:-1]
                coords.append(coords[0])
            else:
                coords.append(coords[0])
        elif start_z != end_z:
            coords = coords[:-1]
            coords.append(coords[0])
        closed_linestring = LineString(coords)
        if not linestring_validation(closed_linestring):
            print("The LineString is not valid for conversion to Polygon.")
            return None
        return Polygon(list(closed_linestring.coords))

    def buffer(linestring: LineString) -> Polygon|None:
        # Create a small buffer around the LineString to form a Polygon
        buffered = linestring.buffer(threshold, resolution=8, cap_style='flat')
        if not isinstance(buffered, Polygon):
            print("Buffering did not result in a valid Polygon.")
            return None
        return buffered

    method_functions = {
        'closed_line': closed_line,
        'buffer': buffer
    }
    polygon = method_functions[method](linestring)
    return polygon

if __name__ == "__main__":
    linje = "POLYGON Z ((266661.72 7037302.14 266661.86, 266661.89 7037302.22 164.19, 266661.88 7037302.42 164.21, 266661.72 7037302.14 266661.86))"
    ls = wkt.loads(linje)
    if not isinstance(ls, LineString):
        raise TypeError("Input must be a LineString geometry.")
    print(linestring_to_polygon(ls))