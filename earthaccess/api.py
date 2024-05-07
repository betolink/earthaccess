import requests
import s3fs
from fsspec import AbstractFileSystem
from typing_extensions import Any, Dict, List, Optional, Union

import earthaccess

from .auth import Auth
from .results import DataCollection, DataGranule
from .search import CollectionQuery, DataCollections, DataGranules, GranuleQuery
from .store import Store
from .utils import _validation as validate
from .widgets import SearchWidget


def _normalize_location(location: Optional[str]) -> Optional[str]:
    """Handle user-provided `daac` and `provider` values.

    These values must have a capital letter as the first character
    followed by capital letters, numbers, or an underscore. Here we
    uppercase all strings to handle the case when users provide
    lowercase values (e.g. "pocloud" instead of "POCLOUD").

    https://wiki.earthdata.nasa.gov/display/ED/CMR+Data+Partner+User+Guide?src=contextnavpagetreemode
    """
    if location is not None:
        location = location.upper()
    return location


def search_datasets(count: int = -1, **kwargs: Any) -> List[DataCollection]:
    """Search datasets using NASA's CMR.

    [https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html)

    Parameters:
        count: Number of records to get, -1 = all
        kwargs (Dict):
            arguments to CMR:

            * **keyword**: case-insensitive and supports wildcards ? and *
            * **short_name**: e.g. ATL08
            * **doi**: DOI for a dataset
            * **daac**: e.g. NSIDC or PODAAC
            * **provider**: particular to each DAAC, e.g. POCLOUD, LPDAAC etc.
            * **temporal**: a tuple representing temporal bounds in the form
              `("yyyy-mm-dd", "yyyy-mm-dd")`
            * **bounding_box**: a tuple representing spatial bounds in the form
              `(lower_left_lon, lower_left_lat, upper_right_lon, upper_right_lat)`

    Returns:
        A list of DataCollection results that can be used to get information about a
            dataset, e.g. concept_id, doi, etc.

    Raises:
        RuntimeError: The CMR query failed.

    Examples:
        ```python
        datasets = earthaccess.search_datasets(
            keyword="sea surface anomaly",
            cloud_hosted=True
        )
        ```
    """
    if not validate.valid_dataset_parameters(**kwargs):
        print(
            "Warning: a valid set of parameters is needed to search for datasets on CMR"
        )
        return []
    if earthaccess.__auth__.authenticated:
        query = DataCollections(auth=earthaccess.__auth__).parameters(**kwargs)
    else:
        query = DataCollections().parameters(**kwargs)
    datasets_found = query.hits()
    print(f"Datasets found: {datasets_found}")
    if count > 0:
        return query.get(count)
    return query.get_all()


def search_data(count: int = -1, **kwargs: Any) -> List[DataGranule]:
    """Search dataset granules using NASA's CMR.

    [https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html)

    Parameters:
        count: Number of records to get, -1 = all
        kwargs (Dict):
            arguments to CMR:

            * **short_name**: dataset short name, e.g. ATL08
            * **version**: dataset version
            * **doi**: DOI for a dataset
            * **daac**: e.g. NSIDC or PODAAC
            * **provider**: particular to each DAAC, e.g. POCLOUD, LPDAAC etc.
            * **temporal**: a tuple representing temporal bounds in the form
              `("yyyy-mm-dd", "yyyy-mm-dd")`
            * **bounding_box**: a tuple representing spatial bounds in the form
              `(lower_left_lon, lower_left_lat, upper_right_lon, upper_right_lat)`

    Returns:
        a list of DataGranules that can be used to access the granule files by using
            `download()` or `open()`.

    Raises:
        RuntimeError: The CMR query failed.

    Examples:
        ```python
        datasets = earthaccess.search_data(
            doi="10.5067/SLREF-CDRV2",
            cloud_hosted=True,
            temporal=("2002-01-01", "2002-12-31")
        )
        ```
    """
    if earthaccess.__auth__.authenticated:
        query = DataGranules(earthaccess.__auth__).parameters(**kwargs)
    else:
        query = DataGranules().parameters(**kwargs)
    granules_found = query.hits()
    print(f"Granules found: {granules_found}")
    if count > 0:
        return query.get(count)
    return query.get_all()


def login(strategy: str = "all", persist: bool = False) -> Auth:
    """Authenticate with Earthdata login (https://urs.earthdata.nasa.gov/).

    Parameters:
        strategy:
            An authentication method.

            * **"all"**: (default) try all methods until one works
            * **"interactive"**: enter username and password.
            * **"netrc"**: retrieve username and password from ~/.netrc.
            * **"environment"**: retrieve username and password from `$EARTHDATA_USERNAME` and `$EARTHDATA_PASSWORD`.
        persist: will persist credentials in a .netrc file

    Returns:
        An instance of Auth.
    """
    if strategy == "all":
        for strategy in ["environment", "netrc", "interactive"]:
            try:
                earthaccess.__auth__.login(strategy=strategy, persist=persist)
            except Exception:
                pass

            if earthaccess.__auth__.authenticated:
                earthaccess.__store__ = Store(earthaccess.__auth__)
                break
    else:
        earthaccess.__auth__.login(strategy=strategy, persist=persist)
        if earthaccess.__auth__.authenticated:
            earthaccess.__store__ = Store(earthaccess.__auth__)

    return earthaccess.__auth__


def download(
    granules: Union[DataGranule, List[DataGranule], str, List[str]],
    local_path: Optional[str] = None,
    provider: Optional[str] = None,
    threads: Optional[int] = 8,
) -> List[str]:
    """Retrieves data granules from a remote storage system.

       * If we run this in the cloud, we will be using S3 to move data to `local_path`.
       * If we run it outside AWS (us-west-2 region) and the dataset is cloud hosted,
            we'll use HTTP links.

    Parameters:
        granules: a granule, list of granules, a granule link (HTTP), or a list of granule links (HTTP)
        local_path: local directory to store the remote data granules
        provider: if we download a list of URLs, we need to specify the provider.
        threads: parallel number of threads to use to download the files, adjust as necessary, default = 8

    Returns:
        List of downloaded files

    Raises:
        Exception: A file download failed.
    """
    provider = _normalize_location(provider)
    if isinstance(granules, DataGranule):
        granules = [granules]
    elif isinstance(granules, str):
        granules = [granules]
    try:
        results = earthaccess.__store__.get(granules, local_path, provider, threads)
    except AttributeError as err:
        print(err)
        print("You must call earthaccess.login() before you can download data")
        return []
    return results


def open(
    granules: Union[List[str], List[DataGranule]],
    provider: Optional[str] = None,
    threads: Optional[int] = 8,
    smart_open: Optional[bool] = True,
    fsspec_opts: Optional[Dict[str, Any]] = {},
    quiet: Optional[bool] = False,
) -> List[AbstractFileSystem]:
    """Returns a list of fsspec file-like objects that can be used to access files
    hosted on S3 or HTTPS by third party libraries like xarray.

    Parameters:
        granules: a list of granule instances **or** list of URLs, e.g. `s3://some-granule`.
            If a list of URLs is passed, we need to specify the data provider.
        provider: e.g. POCLOUD, NSIDC_CPRD, etc. only to be used when we pass URLs and not results from search_data
        threads: number of threads to use to open the files, default = 8
        smart_open: if True, it will use block cache to open the files instead of read-ahead see , default = True
        fsspec_opts: options to pass to the fsspec file system, e.g. `{"anon": True}` for anonymous access and more importantly
            we can override the default cache settings.
        quiet: if True, it will not print the progress bar (useful when we work with many open operations).

    Returns:
        a list of https or s3fs "file pointers" to files on NASA AWS buckets.
    """
    provider = _normalize_location(provider)
    results = earthaccess.__store__.open(granules=granules, provider=provider, threads=threads, smart=smart_open, fsspec_opts=fsspec_opts, quiet=quiet)
    return results


def get_s3_credentials(
    daac: Optional[str] = None,
    provider: Optional[str] = None,
    results: Optional[List[DataGranule]] = None,
) -> Dict[str, Any]:
    """Returns temporary (1 hour) credentials for direct access to NASA S3 buckets. We can
    use the daac name, the provider, or a list of results from earthaccess.search_data().
    If we use results, earthaccess will use the metadata on the response to get the credentials,
    which is useful for missions that do not use the same endpoint as their DAACs, e.g. SWOT.

    Parameters:
        daac: a DAAC short_name like NSIDC or PODAAC, etc.
        provider: if we know the provider for the DAAC, e.g. POCLOUD, LPCLOUD etc.
        results: List of results from search_data()

    Returns:
        a dictionary with S3 credentials for the DAAC or provider
    """
    daac = _normalize_location(daac)
    provider = _normalize_location(provider)
    if results is not None:
        endpoint = results[0].get_s3_credentials_endpoint()
        return earthaccess.__auth__.get_s3_credentials(endpoint=endpoint)
    return earthaccess.__auth__.get_s3_credentials(daac=daac, provider=provider)


def collection_query() -> CollectionQuery:
    """Returns a query builder instance for NASA collections (datasets).

    Returns:
        a query builder instance for data collections.
    """
    if earthaccess.__auth__.authenticated:
        query_builder = DataCollections(earthaccess.__auth__)
    else:
        query_builder = DataCollections()
    return query_builder


def granule_query() -> GranuleQuery:
    """Returns a query builder instance for data granules

    Returns:
        a query builder instance for data granules.
    """
    if earthaccess.__auth__.authenticated:
        query_builder = DataGranules(earthaccess.__auth__)
    else:
        query_builder = DataGranules()
    return query_builder


def get_fsspec_https_session() -> AbstractFileSystem:
    """Returns a fsspec session that can be used to access datafiles across many different DAACs.

    Returns:
        An fsspec instance able to access data across DAACs.

    Examples:
        ```python
        import earthaccess

        earthaccess.login()
        fs = earthaccess.get_fsspec_https_session()
        with fs.open(DAAC_GRANULE) as f:
            f.read(10)
        ```
    """
    session = earthaccess.__store__.get_fsspec_session()
    return session


def get_requests_https_session() -> requests.Session:
    """Returns a requests Session instance with an authorized bearer token.
    This is useful for making requests to restricted URLs, such as data granules or services that
    require authentication with NASA EDL.

    Returns:
        An authenticated requests Session instance.

    Examples:
        ```python
        import earthaccess

        earthaccess.login()

        req_session = earthaccess.get_requests_https_session()
        data = req_session.get(granule_url, headers = {"Range": "bytes=0-100"})

        ```
    """
    session = earthaccess.__store__.get_requests_session()
    return session


def get_s3fs_session(
    daac: Optional[str] = None,
    provider: Optional[str] = None,
    results: Optional[DataGranule] = None,
) -> s3fs.S3FileSystem:
    """Returns a fsspec s3fs file session for direct access when we are in us-west-2.

    Parameters:
        daac: Any DAAC short name e.g. NSIDC, GES_DISC
        provider: Each DAAC can have a cloud provider.
            If the DAAC is specified, there is no need to use provider.
        results: A list of results from search_data().
            `earthaccess` will use the metadata from CMR to obtain the S3 Endpoint.

    Returns:
        An authenticated s3fs session valid for 1 hour.
    """
    daac = _normalize_location(daac)
    provider = _normalize_location(provider)
    if results is not None:
        endpoint = results[0].get_s3_credentials_endpoint()
        if endpoint is not None:
            session = earthaccess.__store__.get_s3fs_session(endpoint=endpoint)
            return session
    session = earthaccess.__store__.get_s3fs_session(daac=daac, provider=provider)
    return session


def get_edl_token() -> str:
    """Returns the current token used for EDL.

    Returns:
        EDL token
    """
    token = earthaccess.__auth__.token
    return token


def explore(
    results: List[DataGranule],
    projection: str = "global",
    map: Any = None,
    roi: Dict[str, Any] = {},
) -> Any:
    sw = SearchWidget(projection=projection, map=map)
    return sw.explore(results, roi)


def load_geometry(filepath: str = "") -> Dict[str, Any]:
    try:
        import geopandas
        from shapely.geometry import Polygon
        from shapely.geometry.polygon import orient
        import fiona
        fiona.drvsupport.supported_drivers['kml'] = 'rw' # enable KML support which is disabled by default
        fiona.drvsupport.supported_drivers['KML'] = 'rw' # enable KML support which is disabled by default
    except ImportError:
        raise ImportError(
            "`earthaccess.load_geometry` requires `geopandas` to be be installed"
        )

    def _orient_polygon(coords: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        polygon = orient(Polygon(coords))
        return list(polygon.exterior.coords)

    def _extract_geometry_info(geometry: Dict[str, Any]) -> Dict[str, Any]:
        feature = geometry["features"][0]["geometry"]
        geometry_type = feature["type"]
        coordinates = feature["coordinates"]

        if geometry_type in ["Polygon"]:
            coords = _orient_polygon(coordinates[0])
            coordinates = [(c[0], c[1]) for c in coords]
            return {"polygon": coordinates}
        elif geometry_type in ["Point"]:
            return {"point": coordinates}
        elif geometry_type in ["LineString"]:
            return {"line": coordinates}
        else:
            print("Unsupported geometry type:", geometry_type)
            return {}

    original_gdf = geopandas.read_file(filepath)
    simplified_json = json.loads(original_gdf.simplify(0.01).to_json())

    geom = _extract_geometry_info(simplified_json)
    return geom


def auth_environ() -> Dict[str, str]:
    auth = earthaccess.__auth__
    if not auth.authenticated:
        raise RuntimeError(
            "`auth_environ()` requires you to first authenticate with `earthaccess.login()`"
        )
    return {"EARTHDATA_USERNAME": auth.username, "EARTHDATA_PASSWORD": auth.password}
