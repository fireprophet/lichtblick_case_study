
EXPECTED_COLUMNS = {
    "products": {
        "id", "deleted", "releasedversion","productcode", "productname", "energy", "consumptiontype",
        "modificationdate"
    },
    "prices": {
        "id", "productid", "pricecomponentid", "pricecomponent", "price",
        "unit", "valid_from", "valid_until", "modificationdate"
    },
    "contracts": {
        "id", "type", "energy", "usage", "usagenet", "createdat", "startdate",
        "enddate", "filingdatecancellation", "cancellationreason", "city",
        "status", "productid", "modificationdate"
    }
}
