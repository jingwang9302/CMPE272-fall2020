import yaml
import schedule
import requests
import time
import sys

with open('./%s' % (sys.argv[1])) as file:
    yaml_file = yaml.safe_load(file)

URL = yaml_file['Steps'][0][1]['outbound_url']
METHOD = yaml_file['Steps'][0][1]['method']
SCHEDULER_WHEN = yaml_file['Scheduler']['when']
TYPE = yaml_file['Steps'][0][1]['type']
CONDITION = yaml_file['Steps'][0][1]['condition']['if']['equal']['right']
SCHEDULER_STEP_ID = yaml_file['Scheduler']['step_id_to_execute'][0]
STEPS_DICT = yaml_file['Steps'][0]
ACTION = yaml_file['Steps'][0][1]['condition']['then']['action']
ERROR_DATA = yaml_file['Steps'][0][1]['condition']['else']['data']


def http_client(config):
    response = requests.request(config['method'], config['url'])
    # print(response)
    return response


def run_steps():
    active_step_id = SCHEDULER_STEP_ID
    TYPE = yaml_file['Steps'][0][active_step_id]['type']
    if TYPE == "HTTP_CLIENT":
        config = {
            'method': yaml_file['Steps'][0][active_step_id]['method'],
            'url': yaml_file['Steps'][0][active_step_id]['outbound_url']
        }
        while True:
            response = http_client(config)
            condition = {
                'status_code':
                yaml_file['Steps'][int(active_step_id) - 1][int(
                    active_step_id)]['condition']['if']['equal']['right'],
                'action':
                yaml_file['Steps'][int(active_step_id) - 1][int(
                    active_step_id)]['condition']['then']['action'],
                'action_data':
                yaml_file['Steps'][int(active_step_id) - 1][int(
                    active_step_id)]['condition']['then']['data'],
            }
            # print(condition)

            if response.status_code == condition['status_code']:
                if condition['action'] == '::print':
                    result = condition['action_data']
                    print_result(result, response)
                    return

                if condition['action'] == '::invoke:step:2':
                    config['url'] = yaml_file['Steps'][0][active_step_id][
                        'condition']['then']['data']
                    active_step_id = condition['action'].split(':')[-1]
                    # print(config)
            else:
                print(ERROR_DATA)
    else:
        print("Invalid type")


def print_result(result, response):
    if result == 'http.response.body':
        print(result, ' : ', response.content)
    if result == 'http.response.headers.X-Ratelimit-Limit':
        print(result, ' : ', response.headers['x-ratelimit-limit'])
    if result == 'http.response.headers.content-type':
        print(result, ' : ', response.headers['Content-Type'])


def set_scheduler():
    SCHEDULER_WHEN = yaml_file['Scheduler']['when']
    cron_list = SCHEDULER_WHEN.split()
    mins = cron_list[0]
    hrs = cron_list[1]
    weekdays = cron_list[2]

    # 1 * *
    if mins != '*' and hrs == '*' and weekdays == '*':
        if int(mins) < 0 or int(mins) > 59:
            print("Error, minute number outbound")
            return
        schedule.every(int(mins)).minutes.do(run_steps)

    # * 1 *
    if mins == '*' and hrs != '*' and weekdays == '*':
        if int(hrs) < 0 or int(hrs) > 23:
            print("Error, hour number outbound")
            return
        schedule.every().day.at("%s:25" % hrs).do(run_steps)

    # * * 1
    if mins == '*' and hrs == '*' and weekdays != '*':
        if int(weekdays) < 0 or int(weekdays) > 7:
            print("Error, week number outbound")
            return
        if weekdays == '0' or weekdays == '7':
            schedule.every().sunday.do(run_steps)
        if weekdays == '1':
            schedule.every().monday.do(run_steps)
        if weekdays == '2':
            schedule.every().tuesday.do(run_steps)
        if weekdays == '3':
            schedule.every().wednesday.do(run_steps)
        if weekdays == '4':
            schedule.every().thursday.do(run_steps)
        if weekdays == '5':
            schedule.every().friday.do(run_steps)
        if weekdays == '6':
            schedule.every().saturday.do(run_steps)

    # 23 14 *
    if mins != '*' and hrs != '*' and weekdays == '*':
        if int(mins) < 0 or int(mins) > 59 or int(hrs) < 0 or int(hrs) > 23:
            print("Error, number outbound")
            return
        schedule.every().day().at("%s:%s" % (hrs, mins)).do(run_steps)

    # 1 2 3
    if mins != '*' and hrs != '*' and weekdays != '*':
        if int(mins) < 0 or int(mins) > 59 or int(hrs) < 0 or int(
                hrs) > 23 or int(weekdays) < 0 or int(weekdays) > 7:
            print("Error, number outbound")
            return
        if weekdays == '0' or weekdays == '7':
            schedule.every().sunday.at("%s:%s" % (hrs, mins)).do(run_steps)
        if weekdays == '1':
            schedule.every().monday.at("%s:%s" % (hrs, mins)).do(run_steps)
        if weekdays == '2':
            schedule.every().tuesday.at("%s:%s" % (hrs, mins)).do(run_steps)
        if weekdays == '3':
            schedule.every().wednesday.at("%s:%s" % (hrs, mins)).do(run_steps)
        if weekdays == '4':
            schedule.every().thursday.at("%s:%s" % (hrs, mins)).do(run_steps)
        if weekdays == '5':
            schedule.every().friday.at("%s:%s" % (hrs, mins)).do(run_steps)
        if weekdays == '6':
            schedule.every().saturday.at("%s:%s" % (hrs, mins)).do(run_steps)


def main():
    SCHEDULER_STEP_ID = yaml_file['Scheduler']['step_id_to_execute'][0]
    STEPS_DICT = yaml_file['Steps'][0]
    if (SCHEDULER_STEP_ID in STEPS_DICT.keys()):
        set_scheduler()
    else:
        print('Invilid step ID provided')

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()