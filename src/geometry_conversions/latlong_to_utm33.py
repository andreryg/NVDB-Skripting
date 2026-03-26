from pyproj import CRS, Transformer
from shapely import Point

def latlong_to_utm33(list_of_points: list[tuple[float, float]]) -> list[Point | None]:
    src_crs = CRS.from_epsg(4326)
    dst_crs = CRS.from_epsg(5973)

    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)

    converted_points = []
    for pnt in list_of_points:
        x,y = transformer.transform(pnt[1], pnt[0])
        converted_points.append(Point(x,y))

    return converted_points

if __name__ == "__main__":
    test = [(69.964911,23.278732),(58.4641,8.7235)]
    print(latlong_to_utm33(test))
