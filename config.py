"""
Pipeline-wide constants.

BLOB_URL  — SAS-signed URL to the KNMI hourly data archive on Azure Blob Storage.
            The token is valid until 2026-12-31. To regenerate, create a new SAS token
            in the Azure portal with read permission on the container.
DE_BILT   — Station identifier for De Bilt, the KNMI reference station (Netherlands).
            Used to filter all other stations out of the dataset.
"""

BLOB_URL = (
    "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz"
    "?se=2026-12-31&sp=r&spr=https&sv=2022-11-02&sr=c"
    "&sig=%2BKhfrZ5jd%2BmtdfdFQXFl05ymk7w2botn3EEuqYqbshg%3D"
)
DE_BILT = "260_T_a"
