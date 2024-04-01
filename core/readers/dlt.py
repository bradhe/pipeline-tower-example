import dlt
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.destination import Destination
from typing import (
    Any)
from core.tasks import Task

class Read(Task):

    def __init__(self, taskname):
        super().__init__(taskname)
        # needs to say from_reference("destination"... to work
        self.dltdestination = Destination.from_reference("destination",
                                                         destination_name=self.taskname+"_destination",
                                                         destination_callable=self.read_destination)
        self.dltpipeline = dlt.pipeline(self.taskname+"_destination_pipeline",
                                        destination = self.dltdestination)

        self.clean_state()

        #print("!__init__")



    def read_destination(self, items: TDataItems, table: TTableSchema) -> None:
        tablename = table["name"]
        if tablename not in self.readitems:
            self.readitems[tablename] = []
        self.readitems[tablename].extend(items)
        self.readtableschema[tablename] = table
        #print("!read_destination")

    def clean_state(self):
        self.readitems={}
        self.readtableschema={}

    def do(self, *args:Any, **kwargs: Any):
        self.clean_state()
        self.dltpipeline.run(*args, **kwargs)
        #print("!do")






