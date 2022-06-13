import sys
import json
import requests
import asyncio
import base64
import random
import uuid
import datetime
import argparse
from requests_toolbelt import MultipartEncoder


class DataverseEntryFactory:
    image_collection = [
        "https://www.bdsplumbinginc.com/site/wp-content/uploads/plumbing-pipe-leaking-e1568749873984.jpg",
        "https://live.staticflickr.com/8265/8711516783_f4cdcdf4f5_b.jpg",
        "https://www.maintenance.org/fileSendAction/fcType/0/fcOid/477016224221942198/filePointer/477016224221942226/fodoid/477016224221942220/imageType/LARGE/inlineImage/true/20200625_151011.jpg",
        "http://cliff.hostkansas.com/pffimages3/cam_gear.JPG",
        "https://www.dunlopservice.uk/wp-content/uploads/2016/01/conveyor-belt-repair.jpg",
        "https://www.flexco.com/Files/Resources/Blog/riprepairlargeholeriprepair_main.jpg",
        "https://d2uhsaoc6ysewq.cloudfront.net/20029/Differential-Misc.-Parts-Eaton-DDP40-18171961.jpg",
        "https://www.bcsagear.fr/wp-content/uploads/2018/12/reduction-gearbox-conveyor-belt.jpg",
        "",
        "",
    ]

    taskname_collection = [
        "Fix leaking pump pipe type X200",
        "Broken valve in sector 3",
        "PID controller on temperature sensor not working as expected",
        "MOXA MC1121 has no connectivity to gateway",
        "Conveyor gearbox blocked by unknown part.",
        "Cog belt brittle on 45 conveyor has to be replaced",
        "Potential gear slop detected on conveyor 12 in row 9 - has to be inspected further",
        "Thrust load on compressor too high, potentially caused by unevenly distributed load",
        "Pneumatic actuator MC2910 doesn't develop 100% strength on stress test",
        "Machine shifts packings, rate of holes too high",
        "Wiring installation missing for belt 219",
        "Fairing must be mounted on 182",
        "Install chargers",
        "Install air conditioning system",
        "Install TFP",
    ]

    def __init__(self, tenant, client_id, client_secret, power_apps_org, schema):
        """
        Create automated entries in Dataverse
        """
        self.tenant = tenant
        self.client_id = client_id
        self.client_secret = client_secret
        self.power_apps_org = power_apps_org
        self.scope = f"https://{power_apps_org}.api.crm4.dynamics.com/.default"
        self.schema = schema
        self.token = self._get_token()
        self.is_running = False
        self.batch_size = 10

    def _get_token(self):
        token_url = f"https://login.microsoftonline.com/{self.tenant}/oauth2/v2.0/token"

        post_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope,
        }

        res = requests.post(token_url, data=post_data)
        if res.status_code != 200:
            raise Exception(res)
        data = res.json()
        return data["access_token"]

    def _img_to_base64(self, url):
        return base64.b64encode(requests.get(url).content).decode("utf-8")

    def create_table(self, table_name):
        url = f"https://{self.power_apps_org}.api.crm4.dynamics.com/api/data/v9.2/EntityDefinitions"

        table = {
            "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
            "Attributes": [
                {
                    "AttributeType": "String",
                    "AttributeTypeName": {"Value": "StringType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Task Name",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": True,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "ApplicationRequired",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}taskname",
                    "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "FormatName": {"Value": "Text"},
                    "MaxLength": 300,
                },
                {
                    "AttributeType": "String",
                    "AttributeTypeName": {"Value": "StringType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Mastertask",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}mastertask",
                    "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "FormatName": {"Value": "Text"},
                    "MaxLength": 100,
                },
                {
                    "AttributeType": "String",
                    "AttributeTypeName": {"Value": "StringType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "ContextImage",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}contextimage",
                    "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "FormatName": {"Value": "Text"},
                    "MaxLength": 200,
                },
                {
                    "AttributeType": "String",
                    "AttributeTypeName": {"Value": "StringType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Asset",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}asset",
                    "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "FormatName": {"Value": "Text"},
                    "MaxLength": 300,
                },
                {
                    "AttributeType": "String",
                    "AttributeTypeName": {"Value": "StringType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Task Status",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "ApplicationRequired",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}taskstatus",
                    "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "FormatName": {"Value": "Text"},
                    "MaxLength": 100,
                },
                {
                    "AttributeType": "Integer",
                    "AttributeTypeName": {"Value": "IntegerType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Shift",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}shift",
                    "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
                },
                {
                    "AttributeType": "String",
                    "AttributeTypeName": {"Value": "StringType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "AssignedPersonEmail",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}assignedpersonemail",
                    "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                    "FormatName": {"Value": "Email"},
                    "MaxLength": 150,
                },
                {
                    "AttributeType": "DateTime",
                    "AttributeTypeName": {"Value": "DateTimeType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Started On",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": True,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}startedon",
                    "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                    "Format": "DateAndTime",
                },
                {
                    "AttributeType": "DateTime",
                    "AttributeTypeName": {"Value": "DateTimeType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Completed On",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}completedon",
                    "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                    "Format": "DateAndTime",
                },
                {
                    "AttributeType": "DateTime",
                    "AttributeTypeName": {"Value": "DateTimeType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Created On 2",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}createdon2",
                    "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                    "Format": "DateAndTime",
                },
                {
                    "AttributeType": "Picklist",
                    "GlobalOptionSet@odata.bind": "/GlobalOptionSetDefinitions(77a09b01-41e2-ec11-bb3c-002248a063ad)",
                    "AttributeTypeName": {"Value": "PicklistType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Task Type",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}tasktype",
                    "@odata.type": "Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
                },
                {
                    "AttributeType": "Picklist",
                    "GlobalOptionSet@odata.bind": "/GlobalOptionSetDefinitions(88dab3b1-3bdb-ec11-bb3d-000d3a665ace)",
                    "AttributeTypeName": {"Value": "PicklistType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Role",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}role",
                    "@odata.type": "Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
                },
                {
                    "AttributeType": "Virtual",
                    "AttributeTypeName": {"Value": "ImageType"},
                    "Description": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [],
                    },
                    "DisplayName": {
                        "@odata.type": "Microsoft.Dynamics.CRM.Label",
                        "LocalizedLabels": [
                            {
                                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                                "Label": "Image",
                                "LanguageCode": 1033,
                            }
                        ],
                    },
                    "IsPrimaryName": False,
                    "RequiredLevel": {
                        "Value": "None",
                        "CanBeChanged": True,
                        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
                    },
                    "SchemaName": f"{self.schema}image",
                    "@odata.type": "Microsoft.Dynamics.CRM.ImageAttributeMetadata",
                },
            ],
            "DisplayCollectionName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": table_name,
                        "LanguageCode": 1033,
                    }
                ],
            },
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": table_name,
                        "LanguageCode": 1033,
                    }
                ],
            },
            "HasActivities": False,
            "HasNotes": False,
            "IsActivity": False,
            "OwnershipType": "UserOwned",
            "SchemaName": f"{self.schema}{table_name}",
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        res = requests.post(url, json=table, headers=headers)
        print(res.status_code)
        if int(res.status_code / 100) != 2:
            raise Exception(res.text)

    async def start(
        self,
        table_name,
        assignee_email,
        no_of_entries=1,
        active_ratio=0.5,
        start_date_range="2022-03-01",
        end_date_range="2022-06-16",
    ):
        """
        Starts putting tasks into the Dataverse table in a loop. It will call the Dataverse REST API
        """

        start_timestamp = int(
            datetime.datetime.strptime(start_date_range, "%Y-%m-%d").timestamp()
        )
        end_timestamp = int(
            datetime.datetime.strptime(end_date_range, "%Y-%m-%d").timestamp()
        )

        for i in range(0, no_of_entries):
            taskname = self.taskname_collection[
                random.randint(0, len(self.taskname_collection) - 1)
            ]
            image = self.image_collection[
                random.randint(0, len(self.image_collection) - 1)
            ]

            new_created_timestamp = random.randint(start_timestamp, end_timestamp)
            new_created_date = datetime.datetime.utcfromtimestamp(new_created_timestamp)

            created_date_str = new_created_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            started_date_str = None
            completed_date_str = None

            if random.random() > active_ratio:
                random_started_minutes = random.randint(60, 10000)
                new_started_date = new_created_date + datetime.timedelta(
                    minutes=random_started_minutes
                )

                random_completed_minutes = random.randint(60, 10000)
                new_completed_date = new_started_date + datetime.timedelta(
                    minutes=random_completed_minutes
                )
                started_date_str = new_started_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                completed_date_str = new_completed_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            self.add_task(
                table_name,
                taskname,
                assignee_email,
                image,
                created_date_str,
                started_date_str,
                completed_date_str,
            )

    async def stop(self):
        self.is_running = False

    def add_task(
        self,
        table_name,
        name,
        email,
        image_url=None,
        created_on=None,
        started_on=None,
        completed_on=None,
        shift=1,
        role=502810001,
    ):
        """
        Puts a single task row into Dataverse table. It will call the Dataverse REST API
        """
        suffix = ""
        if table_name.lower()[len(table_name) - 1] == "s":
            suffix = "es"
        else:
            suffix = "s"

        batch_url = (
            f"https://{self.power_apps_org}.api.crm4.dynamics.com/api/data/v9.2/$batch"
        )
        url = f"https://{self.power_apps_org}.api.crm4.dynamics.com/api/data/v9.2/{self.schema}{table_name.lower()}{suffix}"
        id = str(uuid.uuid4())

        task_status = "Active"
        if completed_on is not None:
            task_status = "Inactive"

        image = None
        if not (image_url is None or image_url == ""):
            image = self._img_to_base64(image_url)

        entry = {
            f"{self.schema}taskname": name,
            f"{self.schema}{table_name.lower()}id": id,
            f"{self.schema}tasktype": 0,  # 0 = Standard, 1 = 5S, 2 = Kaizen, 3 = Subtask
            f"{self.schema}contextimage": "",
            f"{self.schema}taskstatus": task_status,
            f"{self.schema}image": image,
            f"{self.schema}createdon2": created_on,
            f"{self.schema}startedon": started_on,
            f"{self.schema}completedon": completed_on,
            f"{self.schema}asset": "",
            f"{self.schema}assignedpersonemail": email,
            f"{self.schema}shift": shift,
            f"{self.schema}role": role,  # 502810000 = Maintenance technician, 502810001 = Operator
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        # A work in progress to enable batch loading of entries (see https://docs.microsoft.com/en-us/power-apps/developer/data-platform/webapi/execute-batch-operations-using-web-api)
        # batch_boundary = "batch_AAA123"

        # data = f"""--{batch_boundary}\nContent-Type: application/http\nContent-Transfer-Encoding:binary\n\nGET {self.power_apps_org}.api.crm4.dynamics.com/api/data/v9.2/{self.schema}{table_name.lower()}{suffix} HTTP/1.1\nAccept: application/json\n\n{json.dumps(entry)}
        # --{batch_boundary}--"""

        # data = data.replace("None", "null")

        # res = requests.post(
        #     batch_url,
        #     headers={
        #         "Content-Type": "multipart/mixed;boundary=" + batch_boundary,
        #         "Authorization": f"Bearer {self.token}",
        #     },
        #     data=data,
        # )

        res = requests.post(url, json=entry, headers=headers)
        print(res.status_code)
        if int(res.status_code / 100) != 2:
            raise Exception(res.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(
        help="Create a dataverse table and/or fill it with random values"
    )

    create_parser = subparsers.add_parser("create")
    fill_parser = subparsers.add_parser("fill")

    create_parser.add_argument("creationtype", choices=["dataversetable"])
    create_parser.add_argument("--aad_tenant", "-t", type=str, required=True)
    create_parser.add_argument("--aad_client_id", "-i", type=str, required=True)
    create_parser.add_argument("--aad_client_secret", "-p", type=str, required=True)
    create_parser.add_argument("--power_apps_org", "-o", type=str, required=True)
    create_parser.add_argument(
        "--schema",
        "-s",
        type=str,
        required=True,
        help="Schema used as a prefix for the table",
    )
    create_parser.add_argument(
        "--table_name",
        "-n",
        type=str,
        required=True,
        help="Name of the Dataverse table that should be added",
    )

    fill_parser.add_argument("--aad_tenant", "-t", type=str, required=True)
    fill_parser.add_argument("--aad_client_id", "-i", type=str, required=True)
    fill_parser.add_argument("--aad_client_secret", "-p", type=str, required=True)
    fill_parser.add_argument("--power_apps_org", "-o", type=str, required=True)
    fill_parser.add_argument("--schema", "-s", type=str, required=True)
    fill_parser.add_argument("--table_name", "-n", type=str, required=True)
    fill_parser.add_argument(
        "--assignee_email",
        type=str,
        required=False,
        default=None,
        help="The email address of the person resposible for task completion",
    )
    fill_parser.add_argument(
        "--no_of_entries",
        type=int,
        required=False,
        default=1,
        help="The number of tasks that should be added to the table with random data",
    )
    fill_parser.add_argument(
        "--active_ratio",
        type=float,
        required=False,
        default=0.5,
        help="Ratio of tasks that will get the status 'Active'",
    )
    fill_parser.add_argument(
        "--start_date_range",
        type=str,
        required=False,
        default="2022-01-01",
        help="Randomly picked task creation dates will start no sooner than this date",
    )
    fill_parser.add_argument(
        "--end_date_range",
        type=str,
        required=False,
        default="2022-07-01",
        help="Randomly picked task creation dates will end no sooner than this date ",
    )

    args = parser.parse_args()

    dvef = DataverseEntryFactory(
        args.aad_tenant,
        args.aad_client_id,
        args.aad_client_secret,
        args.power_apps_org,
        args.schema,
    )

    if sys.argv[1] == "create":
        if args.creationtype == "dataversetable":
            dvef.create_table(args.table_name)
    elif sys.argv[1] == "fill":
        fun = dvef.start(
            args.table_name,
            args.assignee_email,
            args.no_of_entries,
            args.active_ratio,
            args.start_date_range,
            args.end_date_range,
        )
        asyncio.run(fun)
