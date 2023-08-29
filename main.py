import requests
from pprint import pprint
import pynetbox
import datetime
import urllib3

import config

# Disable warning message, if you don't use valid ssl cert in Veeam
urllib3.disable_warnings()

#####################
#  VEEAM SETTINGS   #
#####################

veeamApiUrl = config.veeamApiUrl

veeamLogin = config.veeamLogin
veeamPassword = config.veeamPassword

endpoints = {
    'get.AccessToken': '/api/oauth2/token',
    'get.BackupObjects': '/api/v1/backupObjects?limit=10000',  # I don't use batches, be careful
    'get.BackupObjectRestorePoints': '/api/v1/backupObjects/{}/restorePoints'
}

#####################
#  NETBOX SETTINGS  #
#####################

nb = pynetbox.api(
    config.netboxUrl,
    token=config.netboxApiToken
)


#####################
# VEEAM API METHODS #
#####################

# Generate full url based on endpoints
def getUrl(api_action):
    link = endpoints.get(api_action)
    url = veeamApiUrl + link
    return url


# Authorization by password, get a token
def passwordAuth(username, password, end='get.AccessToken'):
    url = getUrl(end)
    headers = {"x-api-version": "1.0-rev2",
               "Content-Type": "application/x-www-form-urlencoded"}
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }
    response = requests.post(url, headers=headers, data=data, verify=False)
    return response.json()


# Get a list of all backup objects
def getBackupObjects(token=passwordAuth(veeamLogin, veeamPassword).get('access_token'), end='get.BackupObjects'):
    url = getUrl(end)
    headers = {"x-api-version": "1.0-rev2",
               "Content-Type": "application/json",
               "Authorization": "Bearer " + str(token)}
    response = requests.get(url, headers=headers, verify=False)
    return (response.json()).get('data')


# Get a list of all restore points of one backup object by ID
def getBackupObjectRestorePoints(ids,
                                 token=passwordAuth(veeamLogin, veeamPassword).get('access_token'),
                                 end='get.BackupObjectRestorePoints'):
    url = getUrl(end).format(ids)
    headers = {"x-api-version": "1.0-rev2",
               "Content-Type": "application/json",
               "Authorization": "Bearer " + str(token)}
    response = requests.get(url, headers=headers, verify=False)
    return response.json()


######################
# NETBOX API METHODS #
######################

# We get all virtual machines that satisfy the filter. We cannot edit their properties.
# Return example: [srv-app1, srv-app2, ...]
def getNetboxVMs():
    vms = list(nb.virtualization.virtual_machines.filter(status='active', cf_backup_plan=['Да', 'Нет']))
    return vms


# We get a specific virtual machine with the ability to edit.
def getNetboxVM(name):
    vm = nb.virtualization.virtual_machines.get(status='active', name=name)
    return vm


####################
# GENERAL METHODS #
####################

# Create structure ['srv-app1': ['BackupObjId1', 'BackupObjId2', ...], ...]
def getBackupsIdList(backupObjects):
    # Get a list of unique virtual machines names from the Veeam
    vm_names = []
    for record in backupObjects:
        if record['name'] not in vm_names:
            vm_names.append(record['name'])

    result = {}
    for vm_name in vm_names:
        for record in backupObjects:

            if record['name'] == vm_name:
                if vm_name not in result:
                    result[record['name']] = [record['id']]
                else:
                    result.get(record['name']).append(record['id'])
    return result


# Create a list of all restore points for a specific virtual machine and sort it
# BackupId[0] = Name VM, BackupId[1] = IDs List
def getVmRestorePointsList(BackupIdList):
    rp_list_notSorted = {}
    vm_name = BackupIdList[0]
    vm_id = BackupIdList[1]
    rp_list_notSorted[vm_name] = []
    for id in vm_id:
        rp_list = getBackupObjectRestorePoints(id).get('data')
        for rp in rp_list:
            rp_list_notSorted.get(vm_name).append(rp)

    vm_rp = list(rp_list_notSorted.values())[0]

    rp_list_Sorted = {}
    sorted_list = sorted(
        vm_rp,
        key=lambda x: datetime.datetime.fromisoformat(x['creationTime']), reverse=False)

    rp_list_Sorted[vm_name] = sorted_list

    return rp_list_Sorted


###################
# Run and Execute #
###################

VeeamsBackUpObj = getBackupObjects()
VeeamsBackupsIdList = getBackupsIdList(VeeamsBackUpObj)

today = (datetime.datetime.now()).strftime("%d.%m.%Y")
yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%d.%m.%Y")

for v in VeeamsBackupsIdList.items():

    veeam_vm_name = v[0]
    nb_vm = getNetboxVM(str(veeam_vm_name).lower())
    if nb_vm is not None:

        veeam_vm_rp = getVmRestorePointsList(v)
        veeam_vm_rp_count = len(list(veeam_vm_rp.values())[0])

        if int(veeam_vm_rp_count) != 0:

            # Editing VM page on NetBox
            nb_vm.custom_fields['backup_plan'] = 'Да'
            nb_vm.custom_fields['backup_instances'] = len(v[1])  # Count of different Backup Storages for current VM
            nb_vm.custom_fields['backup_restore_points'] = int(veeam_vm_rp_count)
            nb_vm.custom_fields['backup_old_rp'] = datetime.date.strftime(datetime.datetime.fromisoformat(
                veeam_vm_rp[veeam_vm_name][0].get('creationTime')), "%d.%m.%Y %H:%M:%S")
            nb_vm.custom_fields['backup_last_rp'] = datetime.date.strftime(datetime.datetime.fromisoformat(
                veeam_vm_rp[veeam_vm_name][-1].get('creationTime')), "%d.%m.%Y %H:%M:%S")
            nb_vm.save()

            # Add log to NetBox journal
            for rp in veeam_vm_rp[veeam_vm_name]:
                rp_creationDay = datetime.date.strftime(
                    datetime.datetime.fromisoformat(rp.get('creationTime')), "%d.%m.%Y")  # To compare dates
                rp_creationTime = datetime.date.strftime(
                    datetime.datetime.fromisoformat(rp.get('creationTime')), "%d.%m.%Y %H:%M:%S")  # For NetBox logs
                if yesterday == rp_creationDay:
                    nb.extras.journal_entries.create({'assigned_object_type': 'virtualization.virtualmachine',
                                                      'assigned_object_id': int(nb_vm['id']),
                                                      'kind': 'info',
                                                      'comments': 'Created a restore point {}'.format(
                                                          str(rp_creationTime))})

            print("Edit VM - " + str(veeam_vm_name))
    else:
        pass
