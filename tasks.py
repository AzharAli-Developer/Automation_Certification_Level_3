"""" Robocorp Automation Certification Level 3"""

import requests
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables
from robocorp import workitems
from robocorp.tasks import task


http = HTTP()
json = JSON()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic_data.json"

# JSON data keys
MAX_RATE = 5.0
GENDER_KEY = "Dim1"
YEAR_KEY = "TimeDim"
BOTH_GENDERS = "BTSX"
RATE_KEY = "NumericValue"
COUNTRY_KEY = "SpatialDim"


@task
def produce_traffic_data():
    """Produces traffic data work items."""

    """ Downloads traffic data ."""
    http.download(
        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json",
        target_file=TRAFFIC_JSON_FILE_PATH,
        overwrite=True,
    )
    traffic_data = load_traffic_data_as_table()
    filtered_data = filter_and_sort_traffic_data(traffic_data)
    filtered_data = get_latest_data_by_country(filtered_data)
    payloads = create_work_item_payloads(filtered_data)
    save_work_item_payloads(payloads)


def load_traffic_data_as_table():
    """Load traffic data as table form"""
    json_data = json.load_json_from_file("output/traffic_data.json")
    return table.create_table(json_data["value"])


def filter_and_sort_traffic_data(data):
    """Filter and sort traffic data"""
    table.filter_table_by_column(data, RATE_KEY, "<", MAX_RATE)
    table.filter_table_by_column(data, GENDER_KEY, "==", BOTH_GENDERS)
    table.sort_table_by_column(data, YEAR_KEY, False)
    return data


def get_latest_data_by_country(data):
    """Getting latest data by country"""
    data = table.group_table_by_column(data, COUNTRY_KEY)
    latest_data_by_country = []
    for group in data:
        first_row = table.pop_table_row(group)
        latest_data_by_country.append(first_row)
    return latest_data_by_country


def create_work_item_payloads(traffic_data):
    """Create work items"""
    payloads = []
    for row in traffic_data:
        payload = dict(
            country=row[COUNTRY_KEY],
            year=row[YEAR_KEY],
            rate=row[RATE_KEY],
        )
        payloads.append(payload)
    return payloads


def save_work_item_payloads(payloads):
    """Save work items"""
    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)


@task
def consume_traffic_data():
    """Consumes traffic data work items."""
    for item in workitems.inputs.current.outputs:
        traffic_data = item.payload["traffic_data"]
        if len(traffic_data["country"]) == 3:
            status, return_json = post_traffic_data_to_sales_system(traffic_data)
            if status == 200:
                item.done()
            else:
                item.fail(
                    exception_type="APPLICATION",
                    code="TRAFFIC_DATA_POST_FAILED",
                    message=return_json["message"],
                )
        else:
            item.fail(
                exception_type="BUSINESS",
                code="INVALID_TRAFFIC_DATA",
                message=item.payload,
            )


def post_traffic_data_to_sales_system(traffic_data):
    """Copy traffic data into sales system API"""
    url = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"
    response = requests.post(url, json=traffic_data)
    return response.status_code, response.json()


# Main execution
produce_traffic_data()
consume_traffic_data()
