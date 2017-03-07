/*--------------------------------------------------------------------------------------+
//----------------------------------------------------------------------------
// DOCUMENT ID:   
// LIBRARY:       
// CREATOR:       Mark Anderson
// DATE:          05-05-2016
//
// NAME:          RunSCJob.cs
//
// DESCRIPTION:   Utility to create translation jobs.
//
// REFERENCES:    ProjectWise.
//
// ---------------------------------------------------------------------------
// NOTICE
//    NOTICE TO ALL PERSONS HAVING ACCESS HERETO:  This document or
//    recording contains computer software or related information
//    constituting proprietary trade secrets of Black & Veatch, which
//    have been maintained in "unpublished" status under the copyright
//    laws, and which are to be treated by all persons having acdcess
//    thereto in manner to preserve the status thereof as legally
//    protectable trade secrets by neither using nor disclosing the
//    same to others except as may be expressly authorized in advance
//    by Black & Veatch.  However, it is intended that all prospective
//    rights under the copyrigtht laws in the event of future
//    "publication" of this work shall also be reserved; for which
//    purpose only, the following is included in this notice, to wit,
//    "(C) COPYRIGHT 1997 BY BLACK & VEATCH, ALL RIGHTS RESERVED"
// ---------------------------------------------------------------------------
/*
/* CHANGE LOG
 * $Archive: /ProjectWise/ASFramework/RunSCJob/RunSCJob/RunSCJob.cs $
 * $Revision: 1 $
 * $Modtime: 3/01/17 9:45a $
 * $History: RunSCJob.cs $
 * 
 * *****************  Version 1  *****************
 * User: Mark.anderson Date: 3/06/17    Time: 10:57a
 * Created in $/ProjectWise/ASFramework/RunSCJob/RunSCJob
 * Adding the source files for the RunSCJob
 * 
 * *****************  Version 1  *****************
 * User: Mark.anderson Date: 3/06/17    Time: 10:56a
 * Created in $/ProjectWise/ASFramework/RunSCJob
 * Adding the RunSCJob source to the framework
 * 
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bentley.Orchestration;

using Bentley.Automation;
using Bentley.Automation.JobConfiguration;
using BSI.Common;
using Bentley.Automation.Extensions;
using Bentley.Orchestration.Extensibility;
using Bentley.Orchestration.API;
using Bentley.Automation.Messaging;
using BSI.Orchestration.Utility;
using BSI.Automation;
using HPE.Automation.Extensions;
namespace RunSCJob
{
    /// <summary>
    /// a class to wrap up an item identification pair for processing.  This is done to make
    /// using the list collection easy with a specific class holder.
    /// </summary>
    class PDPair
    {
        public PDPair(int projID, int docID)
        {
            iProject = projID;
            iDocument = docID;
        }
        public int iProject { get; set; }
        public int iDocument { get; set; }
    }
    /// <summary>
    /// this class constructs and submits an Automation Services Job.
    /// </summary>
    class RunSCJob
    {
        static string sDatasource = BPSUtilities.GetSetting("PWDatasourceName");
        static string sUserName = BPSUtilities.GetSetting("PWUser");
        static string sPassword = BPSUtilities.GetSetting("PWPassword");
        static string sPWLoginCMD = BPSUtilities.GetSetting("PWLoginCMD");
        static string sFolder = BPSUtilities.GetSetting("SourceFolder");
        static string sFileName = BPSUtilities.GetSetting("FileName");
        static string sAppName = BPSUtilities.GetSetting("MDLAppName");
        static string sAppKeyin = BPSUtilities.GetSetting("AppKeyin");
        static bool bSaveJob = BPSUtilities.GetBooleanSetting("SaveJob");
        static string sASDataSource = BPSUtilities.GetSetting("ASDataSourceName");
        static string sASProvider = BPSUtilities.GetSetting("ASProvider");
        static string sASCatalog = BPSUtilities.GetSetting("ASCatalog");
        static string sASUser = BPSUtilities.GetSetting("ASUser");
        static string sASPassword = BPSUtilities.GetSetting("ASPassword");
        //static string s_optionbuffer;
        private static string g_sJobNamePrefix = "__" + "HPEGeneralProcessor" + "__Temp__";
        private static void PWErrorCapture()
        {
            String errMsg;
            String errDescrip;
            int errNo;
            errNo = PWWrapper.aaApi_GetLastErrorId();
            errMsg = PWWrapper.aaApi_GetLastErrorMessage();
            errDescrip = PWWrapper.aaApi_GetLastErrorDetail();
            BPSUtilities.WriteLogError(String.Format("PW error {0} Message = {1}, Details = {2}", errNo, errMsg, errDescrip));
            return;
        }
        
        
        /// <summary>
        ///generates a unique job name since a stored job needs a unique name. 
        /// </summary>
        /// <returns>a string value containing a unique name</returns>
        private static string GetUniqueJobName()
        {
            string uniqueName = g_sJobNamePrefix + DateTime.Now + DateTime.Now.Millisecond;
            // get rid of illegal file name characters
            uniqueName = uniqueName.Replace(':', '_');
            uniqueName = uniqueName.Replace('/', '_');
            return uniqueName = uniqueName.Replace('?', '_');
        }
        /// <summary>
        /// adds the ProjectWise connection information to the job definition.
        /// </summary>
        /// <param name="newJob"></param>
        private static void AddPWConnectionInfoToJobDefinition
        (
        JobDefinition newJob
        )
        {
            StringBuilder DataSource = new StringBuilder(100);
            StringBuilder UserName = new StringBuilder(100);
            StringBuilder UserPassword = new StringBuilder(100);
            bool bTemp = false;
            int iTemp = 0;

            if (PWWrapper.aaApi_GetConnectionInfo(PWWrapper.aaApi_GetActiveDatasource(),
                ref bTemp, ref iTemp, ref iTemp, DataSource, 100, UserName,
                    100, UserPassword, 100, null, 0))
            {
                newJob.ProjectWiseDataSource = DataSource.ToString();
                newJob.ProjectWiseUserName = UserName.ToString();
                newJob.ProjectWisePassword = UserPassword.ToString();
                newJob.ProjectWiseDataSourceNativeType = PWWrapper.aaApi_GetActiveDatasourceNativeType();
                newJob.ProjectWiseActiveInterface = PwApiWrapper.dmscli.aaApi_GetActiveInterface();
            }
            else 
            {//put in to assure that the data source information is in the job def.
                newJob.ProjectWiseDataSource = sDatasource;// DataSource.ToString();
                newJob.ProjectWiseUserName = sUserName; //UserName.ToString();
                newJob.ProjectWisePassword = sPassword;// UserPassword.ToString();
                newJob.ProjectWiseDataSourceNativeType = 2;// PWWrapper.aaApi_GetActiveDatasourceNativeType();
                newJob.ProjectWiseActiveInterface = 1; // PwApiWrapper.dmscli.aaApi_GetActiveInterface();
            }
        }
        /// <summary>
        /// adds the document processor information to the job definition.
        /// </summary>
        /// <param name="newJob"> the job being created.</param>
        /// <param name="sDocProcName">the document processor name</param>
        /// <param name="sDocProcDesc">the document processor description</param>
        /// <param name="sDocProcGuidString">the GUID for this document processor</param>
        private static void AddDocumentProcessorToJobDefinition(JobDefinition newJob,
            string sDocProcName, string sDocProcDesc, string sDocProcGuidString)
        {
            DocumentProcessorMetaDataCollection parts = new DocumentProcessorMetaDataCollection();

            DocumentProcessorMetaData p = new DocumentProcessorMetaData();

            p.DocumentProcessorName = sDocProcName;
            p.DocumentProcessorDescription = sDocProcDesc;
            p.DocumentProcessorGuid = sDocProcGuidString;

            parts.Add(p);

            newJob.AddDocumentProcessorMetaDataCollection(parts);
        }


        /// <summary>
        /// creates the job defintion for a list of document pairs.
        /// </summary>
        /// <param name="docids">a list of document id and project id pairs.</param>
        /// <returns></returns>
        private static JobDefinition CreateJob(List<PDPair> docids)
        {
            //create a connection to a datasource.  this will default to the registry for an entry.
            ASDataSource asd = null;
            string constr = string.Format(@"Provider={0}; Data Source={1};Initial Catalog={2};Persist Security Info=True;User ID={3};Password={4};Connect Timeout=30", sASProvider, sASDataSource, sASCatalog, sASUser, sASPassword);
            try
            {
                asd = new ASDataSource(constr, true);
            }
            catch (Exception e)
            {
                BPSUtilities.WriteLogError(string.Format("Error Creating the ASD {0} ", e.ToString()));
                asd = new ASDataSource();
                BPSUtilities.WriteLog(string.Format("connected  to  ASD {0}", asd.ToString()));
                constr = asd.CurrentConnectionString;
                BPSUtilities.WriteLog(constr);
            }
            //create the new instance of the job definition.
            JobDefinition newJob = new JobDefinition(asd);
            newJob.Name = GetUniqueJobName();

            AddPWConnectionInfoToJobDefinition(newJob);

            HPE.Automation.Extensions.HPEGeneralProcessor.ConfigData cfgData =
                new HPE.Automation.Extensions.HPEGeneralProcessor.ConfigData();

            string sDocumentProcessorGuid =
                HPE.Automation.Extensions.HPEGeneralProcessor.Constants.DocumentProcessorGuid.ToLower();

            string sDocumentProcessorName =
                HPE.Automation.Extensions.HPEGeneralProcessor.Constants.DocumentProcessorName;

            cfgData.AppKeyin = sAppKeyin;
            cfgData.MDLAppName = sAppName;
            cfgData.PWLoginCMD = sPWLoginCMD;
            cfgData.PWUser = sUserName;
            cfgData.PWPassword = sPassword;

            newJob.JobType = new JobType(sDocumentProcessorName,
                ASConstants.MultiDocuemtPocessorJobTypeGuid,
                sDocumentProcessorGuid, true);

            AddDocumentProcessorToJobDefinition(newJob, sDocumentProcessorName,
                HPE.Automation.Extensions.HPEGeneralProcessor.Constants.DocumentProcessorDescription,
                sDocumentProcessorGuid);

            newJob.SetCustomData(new Guid(sDocumentProcessorGuid),
                cfgData.ToXmlElement());

            foreach (PDPair pdPair in docids)
            {
                newJob.AddInputSetEntry(pdPair.iDocument, pdPair.iProject, null);
            }
            BPSUtilities.WriteLog(String.Format("Added {0} documents to the job ", docids.Count));
            newJob.LastExecutionEndTime = DateTime.Now.AddMonths(-1);
            newJob.Commit();
            return newJob;
        }
        /// <summary>
        /// kicks the job off to run.
        /// </summary>
        /// <param name="jobdef">AS Job definition</param>
        /// <param name="incremental">a flag to tell if runnning incremental</param>
        /// <returns></returns>
        private static int StartJob(JobDefinition jobdef, bool incremental)
        {
            BPSUtilities.WriteLog(string.Format("Start job {0}", jobdef.Name));
            ASDataSource asd = null;
            string constr = string.Format(@"Provider={0}; Data Source={1};Initial Catalog={2};Persist Security Info=True;User ID={3};Password={4};Connect Timeout=30", sASProvider, sASDataSource, sASCatalog, sASUser, sASPassword);

            try
            {
                asd = new ASDataSource(constr, true);
            }
            catch (Exception e)
            {
                BPSUtilities.WriteLog(string.Format("unable to create a new connection using the registry for information {0}", e.Message));
                asd = new ASDataSource();
            }
            OrchestrationInstance[] ois = OrchestrationInstance.GetOrchestrationInstances(asd);

            OrchestrationInstance oiAS = null;

            foreach (OrchestrationInstance oi in ois)
            {
                System.Diagnostics.Debug.WriteLine(oi.Name);

                if (oi.Name.StartsWith(ASConstants.ProductName))
                {
                    BPSUtilities.WriteLog(string.Format("Found '{0}'", ASConstants.ProductName));
                    oiAS = oi;
                    break;
                }
            }

            if (oiAS != null)
            {
                int rtv = oiAS.SendStartMessage("" + jobdef.ID, incremental);

                return rtv;
            }
            else
            {
                BPSUtilities.WriteLog(string.Format("'{0}' not found", ASConstants.ProductName));
            }


            return -1;
        }
        /// <summary>
        /// delete a job from the queue
        /// </summary>
        /// <param name="iJobToDelete">the job id to remove</param>
        private static void DeleteJob(int iJobToDelete)
        {
            BPSUtilities.WriteLog(string.Format("Deleting job {0}", iJobToDelete));

            try
            {
                JobDefinition.DeleteJobDefinition(new ASDataSource(), iJobToDelete);
                BPSUtilities.WriteLog(string.Format("Deleted job {0} OK", iJobToDelete));
            }
            catch
            {
                BPSUtilities.WriteLog(string.Format("Could not delete job {0}", iJobToDelete));
            }
        }


        /// <summary>
        /// this will generate a document pair list from a folder.
        /// </summary>
        /// <param name="iProjectID"></param>
        /// <param name="docids"></param>
        /// <returns></returns>
        private static int _buildPDPairFromFolder(int iProjectID, List<PDPair> docids)
        {
            int dCount = PwApiWrapper.dmscli.aaApi_GetDocumentCount(iProjectID);
            PwApiWrapper.dmscli.aaApi_SelectDocumentsByProjectId(iProjectID);
            for (int i = 0; i < dCount; ++i)
            {
                int did = PwApiWrapper.dmscli.aaApi_GetDocumentId(i);
                PDPair pair = new PDPair(iProjectID, did);
                docids.Add(pair);
            }
            return dCount;
        }
        /// <summary>
        /// build a document pair from the doc and project id
        /// </summary>
        /// <param name="iDID"></param>
        /// <param name="iPid"></param>
        /// <param name="docids"></param>
        private static void _buildPairFromPandD(int iDID, int iPid, List<PDPair> docids)
        {
            PDPair pair = new PDPair(iPid, iDID);
            docids.Add(pair);
        }
        /// <summary>
        /// gets the document  id  fromthe file name
        /// </summary>
        /// <param name="iProjectId"></param>
        /// <returns></returns>
        private static int _GetDocIDFromName(int iProjectId)
        {
            int iDocId = -1;
            if (!string.IsNullOrEmpty(sFileName))
            {
                if (PwApiWrapper.dmscli.aaApi_SelectDocumentsByNameProp(iProjectId, sFileName, null, null, null) > 0)
                    iDocId = PwApiWrapper.dmscli.aaApi_GetDocumentId(0);

                else
                    BPSUtilities.WriteLog(string.Format("File '{0}' not found in folder '{1}'", sFileName, sFolder));
            }
            return iDocId;
        }
        /// <summary>
        /// build a document list from various options.
        /// </summary>
        /// <param name="iPid">PW Project ID</param>
        /// <param name="iDID">PW document ID</param>
        /// <param name="docids">a list of PW Project Document pairs</param>
        private static void ProcessJob2(int iPid, int iDID, List<PDPair> docids)
        {
            if ((0 == iPid) && (0 == iDID) && (0 == docids.Count))
                return;

           // if (_initialize())
            {
                //these should not be used 
                if ((iPid != 0) && (0 == iDID))
                    _buildPDPairFromFolder(iPid, docids);
                else if ((0 != iDID) && (iPid != 0))
                    _buildPairFromPandD(iDID, iPid, docids);
                //else must have a list of proj and docs  now...
                JobDefinition jd = CreateJob(docids);

                if (-1 != StartJob(jd, BPSUtilities.GetBooleanSetting("Incremental")))
                {
                    BPSUtilities.WriteLog(string.Format("Started job {0} OK", jd.ID));

                    if (!bSaveJob)
                    {
                        JobDefinitionDescriptor[] jobs = JobDefinition.GetDefinitionsAvailable(jd.DataSource);

                        BPSUtilities.WriteLog(string.Format("Found {0} jobs", jobs.Length));

                        List<long> listJobIds = new List<long>();

                        foreach (JobDefinitionDescriptor job in jobs)
                        {
                            if (job.JobDefinitionName.StartsWith(g_sJobNamePrefix))
                            {
                                // default date is 1/1/0001 12:00:00 AM

                                if (job.LastExecuteEndTime > DateTime.Now.AddMonths(-3) && job.LastExecuteEndTime < DateTime.Now)
                                {
                                    BPSUtilities.WriteLog(string.Format("Job {0} finished executing on {1}", job.JobDefinitionID,
                                        job.LastExecuteEndTime));

                                    if (job.Status == JobStatus.Idle)
                                    {
                                        listJobIds.Add(job.JobDefinitionID);
                                    }
                                }
                            }
                        }
                        if (listJobIds.Count > 0)
                        {
                            foreach (long lJobId in listJobIds)
                            {
                                try
                                {
                                    BPSUtilities.WriteLog(string.Format("Trying to delete job {0}", lJobId));
                                    JobDefinition.DeleteJobDefinition(new ASDataSource(), lJobId);
                                    BPSUtilities.WriteLog(string.Format("Deleted job {0} OK", lJobId));
                                }
                                catch
                                {
                                }
                            }
                        }
                    }
                }

                PWWrapper.aaApi_LogoutByHandle(PWWrapper.aaApi_GetActiveDatasource());
            }
            BPSUtilities.WriteLogError("Not logged into Project {0}", "Login Info");
        }
        /// <summary>
        /// logs into projectwise and initializes the connection.
        /// </summary>
        /// <returns>true if it succeeds</returns>
        private static bool _initialize()
        {
            BPSUtilities.WriteLog(String.Format("Initializing "));
            PwApiWrapper.dmscli.aaApi_Initialize(0);

            //try using the embedded login first?
            string localUserName = BPSUtilities.GetSetting("PWUser");
            string localPassword = BPSUtilities.GetSetting("PWPassword");
            if (PwApiWrapper.dmscli.aaApi_Login(0, sDatasource, localUserName, localPassword, ""))
                return true;
            //else we try the one passed along the URL.
            BPSUtilities.WriteLog(string.Format("logging into dsn = {0} with usrname={1} and pwd = {2}", sDatasource, sUserName, sPassword));
            if (PwApiWrapper.dmscli.aaApi_Login(0, sDatasource, sUserName, sPassword, ""))
            {
                BPSUtilities.WriteLog(String.Format("Logged in to {0} as {1} OK", sDatasource, sUserName));
                return true;
            }
            PWErrorCapture();

            IntPtr hData = new IntPtr();
            bool pODBC = false;
            int lpNativeType = 0;
            int lpLoginType = 0;
            StringBuilder dsName = new StringBuilder(1024);
            int dsNameLen = 1024;
            StringBuilder loginName = new StringBuilder(512);
            int lenName = 512;
            StringBuilder pwd = new StringBuilder(1024);
            int pwdLen = 1024;
            StringBuilder schemaName = new StringBuilder(512);
            int snLen = 512;


            if (PwApiWrapper.dmscli.aaApi_GetConnectionInfo(hData, ref pODBC,
                ref lpNativeType, ref lpLoginType, dsName, dsNameLen, loginName,
                lenName, pwd, pwdLen, schemaName, snLen))
            {
                sDatasource = dsName.ToString();
                sUserName = loginName.ToString();
                sPassword = pwd.ToString();

                BPSUtilities.WriteLog("Connected to DSN = " + dsName.ToString() + "  USER= " + loginName.ToString() + " PWD= " + pwd.ToString());
                return true;
            }
            return false;
        }
        /// <summary>
        /// parse the command line to extract the options for the job
        /// </summary>
        /// <param name="args">the name value pairs for the  cmd line options</param>
        /// <returns>number of options</returns>
        static int ParseCMDLine(string[] args, List<PDPair> docids, out int iPID, out int iDID)
        {
            int status = 0;
            bool bShowUsage = false;
            iPID = 0;
            iDID = 0;

            for (int argIndex = 0; argIndex < args.Length; argIndex++)
            {
                status = argIndex;

                if (args[argIndex].ToLower() == "-pwdatasourcename")
                    sDatasource = args[++argIndex];
                else if (args[argIndex].ToLower() == "-pwuser")
                    sUserName = args[++argIndex];
                else if (args[argIndex].ToLower() == "-pwpassword")
                    sPassword = args[++argIndex];
                else if (args[argIndex].ToLower() == "-appname")
                    sAppName = args[++argIndex];
                else if (args[argIndex].ToLower() == "-keyin")
                    sAppKeyin = args[++argIndex];
                else if (args[argIndex].ToLower() == "-pwlogincmd")
                    sPWLoginCMD = args[++argIndex];
                else if (args[argIndex].ToLower() == "-doclist")
                {
                    BPSUtilities.WriteLog(String.Format("Building Document list"));
                    //loop until -doclist is hit again
                    while (args[++argIndex].ToLower() != "-doclist")
                    {
                        string delimStr = ":";
                        char[] delimiter = delimStr.ToCharArray();
                        string[] split = null;
                        string unparsed = args[argIndex];
                        split = unparsed.Split(delimiter);
                        if (split.GetLength(0) >= 2)
                        {
                            int projID;
                            int docID;
                            string p = split[0];
                            string d = split[1];
                            int.TryParse(p, out projID);
                            int.TryParse(d, out docID);
                            PDPair pd = new PDPair(projID, docID);
                            docids.Add(pd);
                            BPSUtilities.WriteLog(String.Format("Added project {0} : Document {1} to the list", projID, docID));
                        }//endif
                    }//whend
                }//endif doclist
 
                else if (args[argIndex].ToLower() == "-debug")
                    System.Diagnostics.Debugger.Launch();

                else if (args[argIndex].ToLower() == "-?" || args[argIndex].ToLower() == "-help" ||
                    args[argIndex].ToLower() == "/help" || args[argIndex].ToLower() == "/?")
                    bShowUsage = true;
            }
            if (bShowUsage)
            {
                Console.WriteLine("Usage: \nDWGRunJob -pwdatasourcename Server:Datasource\n\t[-pwuser User -pwpassword Pass]");
                Console.WriteLine("\t[-doclist projID:docID ... -doclist] the project id and document id to process.  this can be a set of pairs");
            }
            return status;
        }
        /// <summary>
        /// the main entry point to the application.  The information is passed
        /// in as a set of command line arguments.
        /// </summary>
        /// <param name="args">an array of strings that can be parsed into the 
        /// instructions.</param>
        static void Main(string[] args)
        {
            int status=0;
            int iPID = 0;
            int iDID = 0;
            //System.Diagnostics.Debugger.Launch();

            List<PDPair> docids = new List<PDPair>();
            if (args.Length > 1)
                status = ParseCMDLine(args,docids,out iPID,out iDID);
          
            BPSUtilities.WriteLog(String.Format("Parsed {0} arguments",status ));
            for (int i = 0; i < args.Length; i++)
                BPSUtilities.WriteLog(String.Format("Argument {0} is {1}", i, args[i]));
           
            sUserName = BPSUtilities.GetSetting("PWUser");
            sPassword = BPSUtilities.GetSetting("PWPassword");

            if (_initialize())
            {
                ProcessJob2(iPID, iDID, docids);
            }
            else
            {
                BPSUtilities.WriteLogError("ERROR INITIALIZING PW CONNECTION Job will not be processed.");
            }
            System.Environment.Exit(0);
        }
    }
}
