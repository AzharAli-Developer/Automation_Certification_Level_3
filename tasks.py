
"""" Robocorp Automation Certification Level 3"""

from robocorp.tasks import task
from RPA.Browser.Selenium import Selenium
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables


http = HTTP()
json = JSON()
table = Tables()


@task
def produce_traffic_data():
    """ Produces traffic data work items. """
    print("produce")

    """ Downloads traffic data ."""
    http.download(
        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json",
        target_file="output/traffic_data.json",
        overwrite=True,
    )
    traffic_data = load_traffic_data_as_table()

@task
def consume_traffic_data():
    """ Consumes traffic data work items. """
    print("consume")

def load_traffic_data_as_table():
    json_data = json.load_json_from_file("output/traffic_data.json")
    table_from_json = table.create_table(json_data["value"])
    return table_from_json



produce_traffic_data()
consume_traffic_data()