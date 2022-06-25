import os
import json
import logging
import signal
import threading
from itertools import groupby
from operator import itemgetter
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import DescribeInstancesRequest
from aliyunsdkpvtz.request.v20180101.DescribeZonesRequest import DescribeZonesRequest
from aliyunsdkpvtz.request.v20180101.DescribeZoneRecordsRequest import DescribeZoneRecordsRequest
from aliyunsdkpvtz.request.v20180101.AddZoneRecordRequest import AddZoneRecordRequest
from aliyunsdkpvtz.request.v20180101.UpdateRecordRemarkRequest import UpdateRecordRemarkRequest
from aliyunsdkpvtz.request.v20180101.DeleteZoneRecordRequest import DeleteZoneRecordRequest

logging.basicConfig(level=logging.INFO, format='')
load_dotenv()

INTERVAL = int(os.getenv('PVTZ_SYNC_INTERVAL', 60))
REQUEST_NETWORK = os.getenv('PVTZ_SYNC_REQUEST_NETWORK') or 'public'
REGION_ID = os.getenv('PVTZ_SYNC_REGION_ID')
PVTZ_DOMAIN = os.getenv('PVTZ_SYNC_PVTZ_DOMAIN')
PVTZ_REGION_ID = os.getenv('PVTZ_SYNC_PVTZ_REGION_ID') or REGION_ID
PVTZ_RESOURCE_GROUP_ID = os.getenv('PVTZ_SYNC_PVTZ_RESOURCE_GROUP_ID')
ECS_REGION_ID = os.getenv('PVTZ_SYNC_ECS_REGION_ID') or REGION_ID
ECS_ZONE_ID = os.getenv('PVTZ_SYNC_ECS_ZONE_ID')
ECS_VPC_ID = os.getenv('PVTZ_SYNC_ECS_VPC_ID')
ECS_VSWITCH_ID = os.getenv('PVTZ_SYNC_ECS_VSWITCH_ID')
ECS_SECURITY_GROUP_ID = os.getenv('PVTZ_SYNC_ECS_SECURITY_GROUP_ID')
ECS_RESOURCE_GROUP_ID = os.getenv('PVTZ_SYNC_ECS_RESOURCE_GROUP_ID')
ECS_INSTANCE_IDS = os.getenv('PVTZ_SYNC_ECS_INSTANCE_IDS')
ECS_EXCLUDE_INSTANCE_IDS = os.getenv('PVTZ_SYNC_ECS_EXCLUDE_INSTANCE_IDS')
REMARK = os.getenv('PVTZ_SYNC_REMARK') or 'aliyun_pvtz_sync.generated'


class Synchronizer:
  def __init__(self):
    ACCESS_KEY = os.getenv('PVTZ_SYNC_ACCESS_KEY')
    SECRET_KEY = os.getenv('PVTZ_SYNC_SECRET_KEY')
    self.__client = AcsClient(ACCESS_KEY, SECRET_KEY, ECS_REGION_ID)
    self.__cancel = threading.Event()
    self.zone_id = self.get_zone_id()

  def _send_request(self, request):
    request.request_network = REQUEST_NETWORK
    if request.get_product() == 'pvtz' and request.request_network == 'vpc':
      request.endpoint = 'pvtz.vpc-proxy.aliyuncs.com'
    response_str = self.__client.do_action_with_exception(request)
    logging.debug(response_str)
    response_detail = json.loads(response_str)
    return response_detail

  def _ecs_filter_request(self):
    request = DescribeInstancesRequest()
    request.set_PageSize(100)
    if ECS_ZONE_ID: request.set_ZoneId(ECS_ZONE_ID)
    if ECS_VPC_ID: request.set_VpcId(ECS_VPC_ID)
    if ECS_VSWITCH_ID: request.set_VSwitchId(ECS_VSWITCH_ID)
    if ECS_SECURITY_GROUP_ID: request.set_SecurityGroupId(ECS_SECURITY_GROUP_ID)
    if ECS_RESOURCE_GROUP_ID: request.set_ResourceGroupId(ECS_RESOURCE_GROUP_ID)
    if ECS_INSTANCE_IDS: request.set_InstanceIds(json.dumps(ECS_INSTANCE_IDS.split()))
    return request

  def list_instances(self):
    instances = []
    request = self._ecs_filter_request()
    while True:
      response = self._send_request(request)
      instances += response['Instances']['Instance']
      if response['TotalCount'] > response['PageNumber'] * response['PageSize']:
        request.set_PageNumber(request.get_PageNumber() + 1)
      else:
        break
    if ECS_EXCLUDE_INSTANCE_IDS:
      EXCLUDE_INSTANCE_ID_ARRAY = ECS_EXCLUDE_INSTANCE_IDS.split()
      filtered = list(filter(lambda i: i['InstanceId'] not in EXCLUDE_INSTANCE_ID_ARRAY, instances))
      logging.debug(f'Get {len(filtered)} instances (excluded {len(instances) - len(filtered)}).')
      return filtered
    else:
      logging.debug(f'Get {len(instances)} instances.')
      return instances

  def _zone_filter_request(self):
    request = DescribeZonesRequest()
    request.set_SearchMode("EXACT")
    request.set_QueryRegionId(PVTZ_REGION_ID)
    request.set_Keyword(PVTZ_DOMAIN)
    if PVTZ_RESOURCE_GROUP_ID: request.set_ResourceGroupId(PVTZ_RESOURCE_GROUP_ID)
    return request

  def get_zone_id(self):
    request = self._zone_filter_request()
    response = self._send_request(request)
    count = response["TotalItems"]
    if count == 0:
      raise Exception("Cannot find the zone.")
    elif count > 1:
      raise Exception(f'Cannot determine zone id between ${count} zones.')
    return response['Zones']['Zone'][0]['ZoneId']

  def _records_filter_request(self):
    request = DescribeZoneRecordsRequest()
    request.set_ZoneId(self.zone_id)
    request.set_PageSize(100)
    return request

  def list_records(self):
    records = []
    request = self._records_filter_request()
    while True:
      response = self._send_request(request)
      records += response['Records']['Record']
      if response['TotalPages'] > response['PageNumber']:
        request.set_PageNumber(request.get_PageNumber() + 1)
      else:
        break
    filtered = list(filter(lambda r: r.get('Remark') == REMARK, records))
    logging.debug(f'Get {len(filtered)} auto generated records (excluded {len(records) - len(filtered)} mannual records).')
    return filtered

  def _expected(self):
    instances = self.list_instances()
    result = {}
    for i in instances:
      result.setdefault(i['HostName'], []).extend(i['VpcAttributes']['PrivateIpAddress']['IpAddress'])
    result = sorted(result.items())
    for _, ips in result:
      ips.sort()
    return result

  def _current(self):
    records = self.list_records()
    result = [(key, list(group)) for key, group in groupby(sorted(records, key=itemgetter('Rr', 'Value')), itemgetter('Rr'))]
    return result

  def _add(self, name, ip):
    self._added += 1
    logging.info(f'Add record: {name}.{PVTZ_DOMAIN} -> {ip}.')
    request = AddZoneRecordRequest()
    request.set_ZoneId(self.zone_id)
    request.set_Rr(name)
    request.set_Type('A')
    request.set_Value(ip)
    response = self._send_request(request)
    if response.get('Success') is not True:
      logging.warn(f'Add record failed, RequestId: {response.get("RequestId")}.')
    else:
      request = UpdateRecordRemarkRequest()
      request.set_RecordId(response['RecordId'])
      request.set_Remark(REMARK)
      response = self._send_request(request)

  def _remove(self, record_id, name=None, ip=None):
    self._removed += 1
    logging.info(f'Remove record ({record_id}): {name}.{PVTZ_DOMAIN} -> {ip}.')
    request = DeleteZoneRecordRequest()
    request.set_RecordId(record_id)
    response = self._send_request(request)
    if response.get('RecordId') != record_id:
      logging.warn(f'Remove record failed, RequestId: {response.get("RequestId")}.')

  def _syncIp(self, name, expected, current):
    iexp = iter(expected)
    icur = iter(current)
    exp = next(iexp, None)
    cur = next(icur, None)
    while exp is not None or cur is not None:
      if exp is None:
        self._remove(cur['RecordId'], cur['Rr'], cur['Value'])
        cur = next(icur, None)
      elif cur is None:
        self._add(name, exp)
        exp = next(iexp, None)
      elif exp > cur['Value']:
        self._remove(cur['RecordId'], cur['Rr'], cur['Value'])
        cur = next(icur, None)
      elif exp < cur['Value']:
        self._add(name, exp)
        exp = next(iexp, None)
      else:
        exp = next(iexp, None)
        cur = next(icur, None)

  def sync(self):
    self._added = 0
    self._removed = 0
    expected = self._expected()
    current = self._current()
    iexp = iter(expected)
    icur = iter(current)
    exp = next(iexp, None)
    cur = next(icur, None)
    while exp is not None or cur is not None:
      if exp is None:
        for record in cur[1]:
          self._remove(record['RecordId'], record['Rr'], record['Value'])
        cur = next(icur, None)
      elif cur is None:
        name = exp[0]
        for ip in exp[1]:
          self._add(name, ip)
        exp = next(iexp, None)
      elif exp[0] > cur[0]:
        for record in cur[1]:
          self._remove(record['RecordId'], record['Rr'], record['Value'])
        cur = next(icur, None)
      elif exp[0] < cur[0]:
        name = exp[0]
        for ip in exp[1]:
          self._add(name, ip)
        exp = next(iexp, None)
      else:
        self._syncIp(exp[0], exp[1], cur[1])
        exp = next(iexp, None)
        cur = next(icur, None)
    if self._added or self._removed:
      logging.info(f'Sync successful: Added {self._added} records, removed {self._removed} records.')
    else:
      logging.info(f'Sync checked: it\'s already updated.')

  def start(self):
    self.__cancel.clear()
    while not self.__cancel.is_set():
      self.sync()
      self.__cancel.wait(INTERVAL)

  def stop(self):
    self.__cancel.set()


def main():
  synchronizer = Synchronizer()

  def shutdown_handler(signalnum, frame):
    synchronizer.stop()
    logging.info(f'Safe shutdown ({signalnum}).')

  for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
    signal.signal(sig, shutdown_handler)

  synchronizer.start()


if __name__ == '__main__':
  main()
