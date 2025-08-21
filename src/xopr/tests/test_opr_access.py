import pytest
import time

import xopr

def test_get_collections():
    """
    Test that the get_collections function returns a non-empty list of collections.
    """
    opr = xopr.OPRConnection()
    collections = opr.get_collections()
    assert len(collections) > 0, "Expected non-empty list of collections"
    print(f"Found {len(collections)} collections: {collections}")
    for c in collections:
        assert isinstance(c['id'], str), f"Collection id should be a string, got {type(c['id'])}"

def test_get_flights(collection='2017_Antarctica_P3'):
    """
    Test that the get_flights function returns a non-empty list of flights.
    """
    opr = xopr.OPRConnection()
    flights = opr.get_flights(collection)
    assert len(flights) > 0, "Expected non-empty list of flights"
    print(f"Found {len(flights)} flights: {flights}")
    for f in flights:
        assert f['collection'] == collection, f"Flight collection mismatch: {f['collection']} != {collection}"
        assert 'flight_id' in f, "Flight dictionary should contain 'flight_id' key"

@pytest.mark.parametrize("season,flight_id",
    [
        pytest.param('2022_Antarctica_BaslerMKB', '20230109_01', id='single_season_flight'),
        pytest.param(['2022_Antarctica_BaslerMKB'], '20230109_01', id='single_season_flight_list'),
        pytest.param(['2016_Antarctica_DC8', '2017_Antarctica_P3'], '20161117_06', id='multi_season_flight_list')
    ])
def test_load_season(season, flight_id):
    """
    Test loading frames for a given season or list of seasons.
    This checks if the frames can be loaded correctly and merged into a flight.
    It also verifies that layer information can be retrieved from either the layers db
    or from layer files.
    """
    print(f"Testing loading frames for season(s): {season}")

    opr = xopr.OPRConnection()

    max_frames = 2
    frames = opr.query_frames(seasons=season, flight_ids=flight_id, max_items=max_frames)

    print(f"Found {len(frames)} frames for season(s) {season}")

    assert len(frames) == max_frames, f"Expected {max_frames} frames"

    flight = None
    for product_type in ['CSARP_qlook', 'CSARP_standard']:
        print(f"Loading frames for product type: {product_type}")
        loaded_frames = opr.load_frames(frames, data_product=product_type)
        assert len(loaded_frames) == max_frames, f"Loaded frames for {product_type} do not match expected count"

        merged = opr.merge_flights_from_frames(loaded_frames)
        flight = merged[0]

        season_name = None
        if isinstance(season, list):
            if len(season) == 1:
                season_name = season[0]
        else:
            season_name = season
            
        if season_name:
            assert flight.attrs['season'] == season_name, f"Flight season attribute does not match expected: {flight.attrs['season']} != {season_name}"
        
        assert len(flight.attrs) > 0, "Merged flight should have attributes"

    # Test loading layers
    db_layers_loaded = False
    file_layers_loaded = False

    print("Loading layers from db...")
    try:
        layers = opr.get_layers_db(flight)
        assert len(layers) > 1, "Expected layers to be loaded from database"
        db_layers_loaded = True
    except ValueError as e:
        pass
    
    if not db_layers_loaded:
        print("Loading layers from file...")
        layers = opr.get_layers_files(flight)
        print(layers)
        if len(layers) > 1:
            file_layers_loaded = True
    
    assert db_layers_loaded or file_layers_loaded, "No layers loaded from either database or file"

def test_cache_data(tmp_path):
    """
    Test that data is locally cached after loading.
    """

    n_frames = 2
    print(f"Testing caching of {n_frames} frames...")

    opr = xopr.OPRConnection(cache_dir=str(tmp_path))

    # List contents of the cache directory before loading
    initial_cache_contents = list(tmp_path.iterdir())
    print(f"Initial cache contents: {initial_cache_contents}")
    assert len(initial_cache_contents) == 0, "Cache directory should be empty before loading frames"

    season, flight_id = '2016_Antarctica_DC8', '20161117_06'
    frames = opr.query_frames(seasons=season, flight_ids=flight_id, max_items=n_frames)
    assert len(frames) == n_frames, f"Expected {n_frames} frames for the given season and flight ID"

    tstart = time.time()
    loaded_frames = opr.load_frames(frames, data_product='CSARP_qlook')
    t_load_first = time.time() - tstart

    print(f"First load time: {t_load_first:.2f} seconds")
    print(f"Cache contents after first load: {list(tmp_path.iterdir())}")
    assert len(list(tmp_path.iterdir())) > 0, "Cache directory should not be empty after loading frames"

    assert len(loaded_frames) == n_frames, f"Expected {n_frames} loaded frames"
