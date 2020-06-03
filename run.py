from install.wldinstall import installwld
from common.postgresdbcreation import dbcreation
import re,shutil,glob
#import install.wldistall
from config.config import get_config
from faker import Faker
from collections import OrderedDict
from coredatagen.datagen import datagenerate
from wldjob import runwldjobs
from common import copyFilesToHdfs,genmetadata,getderivedFiles,getStats,postgresdbcreation,transaction,getTagIds
from api.wldapi import wldapiCalls
#getCookie,getAgentKey,createDtaSource,executewaterlinejob,getResourceKey,createParentTag,createTagDomain,createTag
import sys,os
SNAP_PREF="R"
COMP_PREF="C"
goldendata=sys.argv[5]
result=sys.argv[6]


def deletefiles(sourcedir, fileext):
	filelist = [f for f in os.listdir(sourcedir) if f.endswith(fileext)]
	for f in filelist:
		# print (os.path.join(sourcedir, f))
		os.remove(os.path.join(sourcedir, f))


if __name__ == '__main__':
	if os.path.exists(result):
		print("Results directory exists")
		for sub in glob.glob(os.getcwd() + "/results/*"):
			shutil.rmtree(sub, ignore_errors=True)
	else:
		os.makedirs(result)
	if os.getenv("JAVA_HOME") == None:
		print("JAVA HOME not set")
	else:
		os.environ["JAVA_HOME"] = get_config()["global"]["javahome"]
	########installation of wld############
	#if wldinstall.lower()=='true':
	install = installwld()
	hostname, ip = install.getHostname()
	if sys.argv[2] == 'true':
		# install=installwld()
		# hostname,ip=install.getHostname()
		print("cleaning up postgres....!!!")
		db = dbcreation()
		# ssh=db.connect(postgreshostname,dbusername,dbpassword)
		db.createPostgresDatabase(get_config()["install"]["postgresdbname"],
								  get_config()["install"]["postgreshostname"], get_config()["install"]["pusername"],
								  get_config()["install"]["ppassword"])
		print("Killing existing wld agent,webserver,meta-server")
		install.killWld()
		install.cleanUpSolr(get_config()["install"]["solrhostname"], get_config()["install"]["solrcollection"])
		if os.path.isfile("./silent.properties_auto"):
			# hostname,ip=install.getHostname()
			files = [f for f in os.listdir(get_config()["install"]["target"]) if
					 re.match(r'^[a-z]+\-agent+-\d+\.2\.[a-z]+', f) or re.match(
						 r'^[a-z]+\-meta+[a-z]+-[a-z]+-\d+.2.[a-z]+', f) or re.match(
						 r'^[a-z]+\-app-[a-z]+-\d+.2.[a-z]+', f)]
			print(files)
			if len(files) == 3:
				install.silent_install_Webserver(get_config()["install"]["target"], hostname)
				install.install_MetaServer(get_config()["install"]["target"], hostname,
										   get_config()["install"]["solrhostname"],
										   get_config()["install"]["solrcollection"])
				install.install_Agent(get_config()["install"]["target"], ip)
			else:
				print(
					"pls check waterline run files don't exists\n this installion only supports build with 2019.2 only!!!")
		else:
			print("silent.properties_auto file doesnt exists")

	######generating base file ####
	if sys.argv[3] == "true":
		deletefiles(goldendata, ".txt")
		deletefiles(goldendata, ".csv")
		if get_config()["global"]["columns"] != 0 and get_config()["global"]["rows"] != 0:
			gen = datagenerate()
			if sys.argv[1] == "auto":
				for i in range(3):
					gen.autodatagen(get_config()["global"]["columns"], get_config()["global"]["rows"], i)
			elif sys.argv[1] == "custom":
				gen.customdataGen(get_config()["global"]["rows"], get_config()["global"]["columns"],
								  OrderedDict(get_config()["columns"]), get_config()["mapping"])
			else:
				print("pass auto or custom as argument")
				sys.exit()
		else:
			print("check for column count and record count !!! looks like value is set to 0")

		##########Get derived files from base file ####
		# print dir(getderivedFiles)
		getdervived = getderivedFiles.getDerivedFiles()

		# derived=getdervived()
		for f in glob.glob("base*.csv"):
			if get_config()["overlap"]["hoverlap"].lower() == 'true' and get_config()["overlap"][
				"hoverlap"].lower() == 'enable':
				# if len(get_config()["overlap"]["hpercentage"])!=0 and get_config()["global"]["rows"]!=0 and get_config()["overlap"]["hoverlapfiles"]!=0 and get_config()["overlap"]["fields"]!='' and  get_config()["overlap"]["ocolumns"]!=0 and len(get_config()["overlap"]["vpercentages"]) !=0:
				print
				get_config()["overlap"]["hpercentage"], get_config()["global"]["rows"], get_config()["overlap"][
					"hoverlapfiles"]
				getdervived.horizantaloverlap(get_config()["overlap"]["hpercentage"], get_config()["global"]["rows"],
											  get_config()["overlap"]["hoverlapfiles"], f)
				getdervived.verticalOverlap(get_config()["overlap"]["fields"], get_config()["overlap"]["ocolumns"],
											get_config()["global"]["rows"], get_config()["overlap"]["vpercentages"],
											get_config()["overlap"]["voverlapf1"],
											get_config()["overlap"]["vnoofoverlapfiles"], f)
			# else:
			#	print ("Failed to generate horizantal derived files")
			elif get_config()["overlap"]["voverlapstatus"].lower() == "enable":
				# if get_config()["overlap"]["fields"]!='' and get_config()["overlap"]["ocolumns"] !=0 and get_config()["global"]["rows"]!=0 and len(get_config()["overlap"]["vpercentages"]) !=0 :
				getdervived.verticalOverlap(get_config()["overlap"]["fields"], get_config()["overlap"]["ocolumns"],
											get_config()["global"]["rows"], get_config()["overlap"]["vpercentages"],
											get_config()["overlap"]["voverlapf1"],
											get_config()["overlap"]["vnoofoverlapfiles"], f)
			# else:
			#	print ("Failed to generate vertical derived files")
			else:
				#	if len(get_config()["overlap"]["hpercentage"])!=0 and get_config()["global"]["rows"]!=0 and get_config()["overlap"]["hoverlapfiles"]!=0:
				getdervived.horizantaloverlap(get_config()["overlap"]["hpercentage"], get_config()["global"]["rows"],
											  get_config()["overlap"]["hoverlapfiles"], f)
		# else:
		#	print ("Failed to generate horizantal derived files")
		######################get stats################################
		stats = getStats.getStats()
		dfdict, basedict, listoffiles, alldit = stats.get_stats(get_config()["overlap"]["vnoofoverlapfiles"],
																get_config()["overlap"]["vpercentages"],
																get_config()["overlap"]["voverlapf1"])
		api = wldapiCalls()
		cookie, login = api.getCookie(get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"],
									  get_config()["wldapi"]["username"], get_config()["wldapi"]["password"])
		cptohdfs = copyFilesToHdfs.copyFilesToHdfs()
		cptohdfs.copytohdfs(get_config()["hdfsdetails"]["hostname"], get_config()["hdfsdetails"]["port"],
							get_config()["hdfsdetails"]["vpath"], get_config()["hdfsdetails"]["sourcepath"])
		agentkey = api.getAgentKey(get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"], cookie, ip)
		# print agentkey[0]
		# pi=wldapiCalls()
		api.createDtaSource(cookie, login, get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"],
							get_config()["hdfsdetails"]["hdfsurl"], get_config()["hdfsdetails"]["hdfspath"],
							agentkey[0], get_config()["hdfsdetails"]["nameofdatasource"])
		from common import genmetadata

		metadata = genmetadata.metadata()
		metadata.derivedGroupingDomain(get_config()["hdfsdetails"]["nameofdatasource"],
									   get_config()["global"]["columns"], get_config()["domain"])
		# mapping=metadata.mappingKeysAndDomains(dfdict,domains,domain)
		gd_path = os.getcwd() + "/snapshot_goldendataset"

		if os.path.exists(gd_path):
			print("path exists %s" % gd_path)
			shutil.rmtree(gd_path)
		# shutil.rmtree(os.getcwd()+"/snapshot")
		else:
			print("golden dataset not available")
		# deletefiles(goldendata,".txt")
		# deletefiles(goldendata,".csv")
		from wldjob.runwldjobs import wldJobs

		job = wldJobs()
		job.runFormat(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
					  get_config()["hdfsdetails"]["hdfspath"])
		job.runSchema(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
					  get_config()["hdfsdetails"]["hdfspath"])
		# if os.path.exists(os.getcwd('./goldendataset')):
		#    if not os.listdir(os.getcwd('./goldendataset')):
		job.TFAPopulate(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["hdfspath"],
						get_config()["install"]["metadata"], "false")
		job.TFASnapshot(get_config()["hdfsdetails"]["wldpath"], os.getcwd() + "/snapshot")
		print("moving snapshot ")
		os.system("mv snapshot  snapshot_goldendataset")
		### Remove all accepted tag asso not seed
		tagidlist = getTagIds.tagIdList()
		if len(tagidlist) != 0:
			for i in tagidlist:
				api.deleteTagAssociations(get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"],
										  cookie, i["id"])
		else:
			print("taglist is empty")
		### run profile###
		job.executewaterlinejob(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
								get_config()["hdfsdetails"]["hdfspath"])

		# job.TFAPopulate(get_config()["hdfsdetails"]["wldpath"],get_config()["hdfsdetails"]["hdfspath"],get_config()["install"]["metadata"],"true")
		job.TFAPopulate(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["hdfspath"],
						os.getcwd() + "/metadata.csv", "true")
		for i in range(1, int(sys.argv[4])):
			job.generateRAScript(get_config()["hdfsdetails"]["wldpath"], goldendata + "/snapshot_goldendataset", result,
								 str(i))
			job.fixTagAssociations(get_config()["hdfsdetails"]["wldpath"], result, str(i))
			job.runTagJob(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
						  get_config()["hdfsdetails"]["typeofdatasource"], get_config()["hdfsdetails"]["hdfsurl"],
						  get_config()["hdfsdetails"]["hdfspath"])
			job.TFASnapshot(get_config()["hdfsdetails"]["wldpath"], result + "/" + SNAP_PREF + str(i))

			job.TFAComparator(get_config()["hdfsdetails"]["wldpath"], goldendata + "/snapshot_goldendataset", result,
							  SNAP_PREF, COMP_PREF, str(i))
	#		os.system("mv snapshot  snapshot%s" %i)
	else:
		snp = os.getcwd() + "/snapshot"
		if os.path.exists(snp):
			print("path exists %s" %gd_path)
			shutil.rmtree(snp)
		print("Base and Derived files already available")
		stats = getStats.getStats()
		dfdict, basedict, listoffiles, alldit = stats.get_stats(get_config()["overlap"]["vnoofoverlapfiles"],
																get_config()["overlap"]["vpercentages"],
																get_config()["overlap"]["voverlapf1"])
		api = wldapiCalls()
		cookie, login = api.getCookie(get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"],
									  get_config()["wldapi"]["username"], get_config()["wldapi"]["password"])
		cptohdfs = copyFilesToHdfs.copyFilesToHdfs()
		cptohdfs.copytohdfs(get_config()["hdfsdetails"]["hostname"], get_config()["hdfsdetails"]["port"],
							get_config()["hdfsdetails"]["vpath"], get_config()["hdfsdetails"]["sourcepath"])
		agentkey = api.getAgentKey(get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"], cookie, ip)
		# print agentkey[0]
		# pi=wldapiCalls()
		api.createDtaSource(cookie, login, get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"],
							get_config()["hdfsdetails"]["hdfsurl"], get_config()["hdfsdetails"]["hdfspath"],
							agentkey[0], get_config()["hdfsdetails"]["nameofdatasource"])
		from common import genmetadata

		metadata = genmetadata.metadata()
		metadata.derivedGroupingDomain(get_config()["hdfsdetails"]["nameofdatasource"],
									   get_config()["global"]["columns"], get_config()["domain"])
		# mapping=metadata.mappingKeysAndDomains(dfdict,domains,domain)

		from wldjob.runwldjobs import wldJobs

		job = wldJobs()
		job.runFormat(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
					  get_config()["hdfsdetails"]["hdfspath"])
		job.runSchema(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
					  get_config()["hdfsdetails"]["hdfspath"])
		job.TFAPopulate(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["hdfspath"],
						get_config()["install"]["metadata"], "false")
		job.TFASnapshot(get_config()["hdfsdetails"]["wldpath"], os.getcwd() + "/snapshot")
		os.system("mv snapshot  snapshot_goldendataset")
		tagidlist = getTagIds.tagIdList()
		if len(tagidlist) != 0:
			for i in tagidlist:
				api.deleteTagAssociations(get_config()["wldapi"]["wldhostname"], get_config()["wldapi"]["wldport"],cookie, i["id"])
		else:
			print("taglist is empty")
		for i in range(1, int(sys.argv[4])):
			job.generateRAScript(get_config()["hdfsdetails"]["wldpath"], goldendata + "/snapshot_goldendataset", result,
								 str(i))
			job.fixTagAssociations(get_config()["hdfsdetails"]["wldpath"], result, str(i))
			job.runTagJob(get_config()["hdfsdetails"]["wldpath"], get_config()["hdfsdetails"]["nameofdatasource"],
						  get_config()["hdfsdetails"]["typeofdatasource"], get_config()["hdfsdetails"]["hdfsurl"],
						  get_config()["hdfsdetails"]["hdfspath"])
			#                job.TFASnapshot(get_config()["hdfsdetails"]["wldpath"])
			job.TFASnapshot(get_config()["hdfsdetails"]["wldpath"], result + "/" + SNAP_PREF + str(i))
			job.TFAComparator(get_config()["hdfsdetails"]["wldpath"], goldendata + "/snapshot_goldendataset", result,
							  SNAP_PREF, COMP_PREF, str(i))
	#		os.system("mv snapshot  snapshot%s" %i)
	job.TFAResultProcessor(get_config()["hdfsdetails"]["wldpath"], goldendata + "/snapshot_goldendataset", result,
						   COMP_PREF)
