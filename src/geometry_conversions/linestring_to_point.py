from shapely import LineString, Point, wkt

def linestring_to_point(linestring : LineString, method : str = 'geographical_center') -> Point|None:
    """Convert a linestring to a Point

    Converts a Shapely LineString to a Shapely Point using the specified method:
    -  geographical_center: Calculates the centroid of the LineString. (Default)
    -  center_point: Calculates the midpoint of the LineString.
    -  start_point: Uses the starting point of the LineString.
    -  end_point: Uses the ending point of the LineString.
    
    Parameters
    ----------
    linestring : LineString
        The input LineString geometry.
    method : str, default='geographical_center'
        The method to use for conversion. Options are 'geographical_center', 'center_point', 'start_point', 'end_point'.

    Returns
    -------
    Point
        The resulting Point geometry.
    None
        If the LineString is invalid or the method is not recognized.
    """
    def validate_linestring(linestring: LineString) -> bool:
        if not isinstance(linestring, LineString):
            return False
        if len(linestring.coords) < 2:
            return False
        return True
    
    def geographical_center(linestring: LineString) -> Point: # Without height, might need to append the average Z coordinate
        return linestring.centroid
    
    def center_point(linestring: LineString) -> Point:
        mid_length = linestring.length / 2
        return linestring.interpolate(mid_length)
    
    def start_point(linestring: LineString) -> Point:
        return Point(linestring.coords[0])
    
    def end_point(linestring: LineString) -> Point:
        return Point(linestring.coords[-1])
    
    if not validate_linestring(linestring):
        print("Invalid LineString geometry.")
        return None
    
    method_functions = {
        'geographical_center': geographical_center,
        'center_point': center_point,
        'start_point': start_point,
        'end_point': end_point
    }
    if method not in method_functions:
        print(f"Method '{method}' is not recognized. Valid methods are: {list(method_functions.keys())}.")
        return None
    
    return method_functions[method](linestring)

if __name__ == "__main__":
    geom = "LINESTRING Z (233846.72 6691541.55 159.884,233844.97 6691542.54 159.874,233842.52 6691538.24 159.914,233844.24 6691537.26 159.874)"
    ls = wkt.loads(geom)
    print(linestring_to_point(ls, method='geographical_center'))  # type: ignore
    print(linestring_to_point(ls, method='center_point')) # type: ignore
    print(linestring_to_point(ls, method='start_point')) # type: ignore
    print(linestring_to_point(ls, method='end_point')) # type: ignore