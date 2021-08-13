from marketorestpython.client import MarketoClient
import json
import csv
import time
import smart_open


class MarketoService:
    def __init__(self):
        self.programIDs = []
        self.jobExportCreationIDs = []
        self.jobReadyIDs = []
        self.munchkin_id = "<OBFUSCATED>"
        self.client_id = "<OBFUSCATED>"
        self.client_secret = "<OBFUSCATED>"
        self.api_limit = None
        self.max_retry_time = None
        self.mc = MarketoClient(self.munchkin_id, self.client_id, self.client_secret, self.api_limit, self.max_retry_time)
        self.fields = ['FirstName','LastName','LeadID','<OBFUSCATED(other fields/attrivutes as well)>']
        self.debugMode = False
        self.jobsAvailableFlag = True

    def enqueue_jobs(self):
        #Queue jobs
        for job in self.jobExportCreationIDs:
            self.mc.execute(method='enqueue_programMembership_export_job', job_id=job)
            print("Enqueueing Program Job {}  . . .".format(job))

    def poll_job_status(self):
        #need to pull in the job id's that we created.
        currentJobs = self.mc.execute(method='get_programMembership_export_jobs_list')
        print("Polling Job Status . . .")
        for currentJob in currentJobs:
            for jobID in self.jobExportCreationIDs:
                #Query all current jobs, we only care about the jobs we created. Check to see if they are completed
                if (currentJob["exportId"] == jobID and currentJob["status"] == "Completed" and jobID not in self.jobReadyIDs):
                    self.jobReadyIDs.append(jobID)
                    print("Job finished {}".format(jobID))


    def transform_file_contents(self,jobFiles):
        transformedFiles = []
        # 3. TRANFORM each file string for an acceptable file writing format
        for fileString in jobFiles:
            transformation = fileString.split('\n')
            transformedFiles.append(transformation)
        return transformedFiles


    def load_file_dictionary(self,transformedFiles):
        #taking the transformed csv, we generate a dictionary
        objectTemplate = {}
        for field in self.fields:
            objectTemplate[field] = None
        masterDictionary = []
        #load each value into dictionary
        programIndex = 0
        while programIndex < len(transformedFiles):
            masterDictionary.append([])
            for row in transformedFiles[programIndex]:
                tempRow = row.split(",")
                for index,value in enumerate(tempRow):
                    objectTemplate[self.fields[index]] = value
                masterDictionary[programIndex].append(objectTemplate)
                objectTemplate = {}
            programIndex = programIndex + 1
        return masterDictionary

    def write_data_to_files(self,masterDictionary):
        programIndex = 0
        while programIndex < len(masterDictionary):
            with smart_open.smart_open("s3://<OBFUSCATED>/programMembership{}.csv".format(self.programIDs[programIndex]), "w") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.fields)
                writer.writeheader()
                for data in masterDictionary[programIndex]:
                    writer.writerow(data)
            programIndex = programIndex + 1

    def getPrograms(self):
        try:
            programs = self.mc.execute(method='get_program_by_tag_type', tagType='Include in Data Extract', tagValue='Yes')
        except KeyError:
            self.jobsAvailableFlag = False
            return
        for program in programs:
            self.programIDs.append(program["id"])
            print("Got Program {} . . .".format(program["id"]))

    def launchJobs(self):
        filterObject = "programId"
        #Create jobs
        for id in self.programIDs:
            print("Creating job for Program: {}".format(id))
            new_export_job_details = self.mc.execute(method='create_programMembership_export_job'.format(object),
                                                     fields=self.fields, filters={filterObject: id})
            self.jobExportCreationIDs.append(new_export_job_details[0]["exportId"])
        self.enqueue_jobs()

    def retrieveJobContents(self):
        jobFiles = []
        for jobID in self.jobReadyIDs:
            jobFiles.append(self.mc.execute(method='get_programMembership_export_job_file',
                                       job_id=jobID).decode("utf-8"))
        transformedFiles = self.transform_file_contents(jobFiles)
        masterDictionary = self.load_file_dictionary(transformedFiles)
        self.write_data_to_files(masterDictionary)

    def pollingJobs(self):
        #The purpose of the sleep fn() is to suspend the thread while the extraction job is being serviced.  Then we run a polling job to review status every 30 seconds
        while (len(self.jobReadyIDs) < len(self.jobExportCreationIDs)):
            self.poll_job_status()
            time.sleep(30)

    def serviceKickOff(self):
        self.getPrograms()
        if self.jobsAvailableFlag is True:
            self.launchJobs()
            self.pollingJobs()
            self.retrieveJobContents()

#built for AWS lambda
def lambda_handler(event, context):
    seed = MarketoService()
    seed.serviceKickOff()

lambda_handler(1,2)