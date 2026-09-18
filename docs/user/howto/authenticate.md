# Authenticate with Earthdata Login

The first step to use NASA Earthdata is to create an account with Earthdata
Login, please follow the instructions at
[NASA EDL](https://urs.earthdata.nasa.gov/)

Once registered, earthaccess can use environment variables, a `.netrc` file or
interactive input from a user to login with NASA EDL.

If a strategy is not specified, environment variables will be used first, then
a `.netrc` (if found, see below), and finally a user's input.

```py
import earthaccess

auth = earthaccess.login()
```

If you have a `.netrc` file (see below) with your Earthdata Login credentials,
you can explicitly specify its use:

```py
auth = earthaccess.login(strategy="netrc")
```

If your Earthdata Login credentials are set as the environment variables
`EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD`, you can explicitly specify their
use:

```py
auth = earthaccess.login(strategy="environment")
```

Alternatively, you can use an existing Earthdata Login token by setting the environment
variable `EARTHDATA_TOKEN` to it and using the same "environment" strategy, above.

If you wish to enter your Earthdata Login credentials when prompted, with
optional persistence to your `.netrc` file (see below), specify the interactive
strategy:

```py
auth = earthaccess.login(strategy="interactive", persist=True)
```

## Authentication

By default, `earthaccess` with automatically look for your EDL account
credentials in two locations:

1. A `.netrc` file: By default, this is either `~/_netrc` (on a Windows system)
   or `~/.netrc` (on a non-Windows system).  On *any* system, you may override
   the default location by setting the `NETRC` environment variable to the path
   of your desired `.netrc` file.

   **NOTE**: When setting the `NETRC` environment variable, there is no
   requirement to use a specific filename.  The name `.netrc` is common, but
   used throughout documentation primarily for convenience.  The only
   requirement is that the *contents* of the file adhere to the
   [`.netrc` file format](https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html).

2. `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` environment variables (or, optionally, `EARTHDATA_TOKEN`
   to use an existing Earthdata Login token)

If neither of these options are configured, you can authenticate by calling the
`earthaccess.login()` method and manually entering your EDL account credentials.

```python
import earthaccess

earthaccess.login()
```

Note you can pass `persist=True` to `earthaccess.login()` to have the EDL
account credentials you enter automatically saved to your `.netrc` file (see
above) for future use.

Once you are authenticated with NASA EDL you can:

* Get a file from a DAAC using a `fsspec` session.
* Request temporary S3 credentials from a particular DAAC (needed to download or
  stream data from an S3 bucket in the cloud).
* Use the library to download or stream data directly from S3.
* Regenerate CMR tokens (used for restricted datasets).

## Earthdata User Acceptance Testing (UAT) environment

If your EDL account is authorized to access the User Acceptance Testing (UAT)
system, you can set earthaccess to work with its EDL and CMR endpoints by
setting the `system` argument at login, as follows:

```python
import earthaccess

earthaccess.login(system=earthaccess.UAT)
```

## Route requests through a proxy

Some environments cannot reach NASA's Earthdata services directly. For example,
in a browser-based (JupyterLite/Pyodide) notebook, [CORS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS)
restrictions prevent earthaccess from calling CMR and Earthdata Login.

If you have a proxy that forwards requests and returns permissive CORS headers,
point earthaccess at it with `earthaccess.set_proxy()`:

```python
import earthaccess

earthaccess.set_proxy("https://earthdata-proxy.example.workers.dev")
```

You can also configure the proxy without writing any code by setting the
`EARTHDATA_PROXY_URL` environment variable; an explicit call to
`earthaccess.set_proxy()` takes precedence over it. Pass `None` to
`earthaccess.set_proxy()` to clear an explicitly configured proxy.

The proxy must accept the fully-qualified target URL as a path suffix, forward
the request, and return the upstream response with permissive CORS headers. For
example, a request for
`https://earthdata-proxy.example.workers.dev/https://cmr.earthdata.nasa.gov/search/granules.umm_json`
is forwarded to
`https://cmr.earthdata.nasa.gov/search/granules.umm_json`.

!!! note

    Only Earthdata Login, CMR, and the Earthdata status service are routed
    through the proxy. Downloading and streaming data from DAACs (over S3 or
    HTTPS) is not.
