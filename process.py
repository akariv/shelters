import dataflows as DF
from pyproj import Proj, Transformer

transformer = Transformer.from_crs("EPSG:2039", "EPSG:4326", always_xy=True)

def location_name(row):
    name = row['name'].strip()
    if name == 'מקלט':
        name = row['Full Address']
    if 'מקלט' not in name:
        name = 'מקלט ' + name
    if row['City'] and row['City'] not in name:
        name = name + ' ' + row['City']
    return name


def convert_coords(row):
    x = row['X']
    y = row['Y']
    lon, lat  = transformer.transform(x, y)
    return [lon, lat]


if __name__ == '__main__':

    DF.Flow(
        DF.load('Mosadot_export.xlsx'),
        # Location Name, Full Address, City, Street, House Number, Comments
        DF.rename_fields({
            'שם יישוב': 'City',
            'שם המוסד': 'name',
            'סטטוס': 'status',
            'כתובת': 'address',
        }),
        DF.filter_rows(lambda r: 'קיים' in r['status']),
        DF.add_field('coordinates', 'array', convert_coords),
        DF.add_field('Full Address', 'string', lambda r: "{address}, {City}".format(**r) if r['City'] else r['address']),
        DF.add_field('Location Name', 'string', location_name),
        DF.add_field('Lat', 'number', lambda r: r['coordinates'][1]),
        DF.add_field('Lon', 'number', lambda r: r['coordinates'][0]),
        DF.select_fields(['Location Name', 'Full Address', 'City', 'Lat', 'Lon']),
        DF.update_resource(-1, name='shelters', path='shelters.csv'),
        DF.dump_to_path('.'),
        DF.printer(),
    ).process()