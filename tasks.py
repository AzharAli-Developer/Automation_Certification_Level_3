
"""" Robocorp Automation Certification Level 3"""

from robocorp.tasks import task
from robocorp import workitems
from RPA.Browser.Selenium import Selenium
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables


http = HTTP()
json = JSON()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic_data.json"

# JSON data keys
COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"
MAX_RATE = 5.0
BOTH_GENDERS = "BTSX"


@task
def produce_traffic_data():
    """ Produces traffic data work items. """
    print("produce")

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

@task
def consume_traffic_data():
    """ Consumes traffic data work items. """
    print("consume")

def load_traffic_data_as_table():
    json_data = json.load_json_from_file("output/traffic_data.json")
    return table.create_table(json_data["value"])

def filter_and_sort_traffic_data(data):
    table.filter_table_by_column(data, RATE_KEY, "<", MAX_RATE)
    table.filter_table_by_column(data, GENDER_KEY, "==", BOTH_GENDERS)
    table.sort_table_by_column(data, YEAR_KEY, False)
    return data

def get_latest_data_by_country(data):
    data = table.group_table_by_column(data, COUNTRY_KEY)
    latest_data_by_country = []
    for group in data:
        first_row = table.pop_table_row(group)
        latest_data_by_country.append(first_row)
    return latest_data_by_country

def create_work_item_payloads(traffic_data):
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
    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)


# Main execution
produce_traffic_data()
consume_traffic_data()