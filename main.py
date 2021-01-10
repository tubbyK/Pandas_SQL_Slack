# system and misc
import os
from pathlib import Path
import pandas as pd
import csv
import datetime
from datetime import datetime as dt, timedelta

# clients
from pydatagateway import datagateway
from slackclient import SlackClient

# custom library: paths and constants
from envobj import *

now = datetime.datetime.now().date()
start_date = now - timedelta(days=now.weekday())
start_date = start_date - timedelta(days=7)
end_date = start_date + timedelta(days=6)

start_date = str(start_date)
end_date = str(end_date)
today_date = str(now)

# Parameters setting

# Presto credentials
username = os.environ.get('presto_username')
password = os.environ.get('presto_password')
host = os.environ.get('presto_host')

# Slack credentials
slack_TOKEN = os.environ.get('slack_TOKEN')

# shared folder paths
fld_path = 'G:/Team Drives/Acquisition/Referrals_users'
shared_folder = os.environ.get('shared_folder')

# file_path
file_name = fld_path + '/referrals_users_' + today_date + '_v1.csv'

# Slack ids
self_id = slack_id_self
channels = slack_channels # e.g. [('channel_name','channel_id'),]
df_channel = pd.DataFrame(channels)
df_channel.columns=['channel','channel_id']

# connect to presto
cursor = datagateway.connect(host=host,
                             username=username,
                             password=password, catalog='hive',
                             schema='public',
                             port='443',
                             protocol='https').cursor()

query = ('''
with campaign_referrals as (
  --get drivers who signed up with user referral code
  select id as referee_id, referral_id as referrer_id, referral_code as sign_up_code_used, referral_type,
          create_at + interval '8' hour as referral_created_time,
          date_trunc('month', create_at + interval '8' hour) as sign_up_month,
          year(create_at + interval '8' hour) as yr,
          month(create_at + interval '8' hour) as mth
    from grab_hitch.user_referral
    where referral_type = 'user'
          --remove this chunk to enable calculation across all time
          --and create_at >= (timestamp '2018-05-01 00:00:00' - interval '8' hour)
          --and create_at < (timestamp '2018-05-01 00:00:00' - interval '8' hour + interval '1' month)
),

  user_details as (
  --get user details
  select id as user_id,
        name,
        phone,
        email,
        create_at + interval '8' hour as create_time_local,
        status
    from grab_hitch.user
    --where taxi_type_id = 130
),

  gh_driver as (
  select user_id as driver_id,
      create_at + interval '8' hour as referrer_create_time,
      application_status
    from grab_hitch.user_auth
    where taxi_type_id = 130
),

  is_DRD as (
  select distinct campaign_referrals.referee_id, gh_driver.referrer_create_time,
        case
          when gh_driver.driver_id is not null and gh_driver.referrer_create_time < campaign_referrals.referral_created_time  then 'DRD'
          else 'PRD'
        end referral_type
    from campaign_referrals
      left join gh_driver on gh_driver.driver_id = campaign_referrals.referrer_id
),

  new_dax_rides as (
  --get driver rides
  select campaign_referrals.referee_id, count(booking_sg_singapore.code) as rides_done
    from grab_hitch.booking_sg_singapore
      right join campaign_referrals on campaign_referrals.referee_id = booking_sg_singapore.driver_id
    where booking_sg_singapore.state_code in (543, 545, 550) and
          pick_up_time between campaign_referrals.referral_created_time and
            (date_trunc('month', campaign_referrals.referral_created_time) + interval '1' month + interval '6' day)
    group by campaign_referrals.referee_id
),

  approved_date as (
  --get driver approved date
    select driver_id, max(approved_) as approved_date
      from (
        select user_auth_approve_sg.user_id as driver_id, user_auth_approve_sg.item, min(user_auth_approve_sg.create_at + interval '8' hour) as approved_
          from grab_hitch.user_auth_approve_sg
          where status = 'approved' and
                item in ('Vehicle', 'DrivingLicense') and
                exists (select driver_id from gh_driver where gh_driver.driver_id = user_auth_approve_sg.user_id)
          group by user_id, item
      )
      group by driver_id
),

  self_booking as (
    select booking_code as code
      from user_trust_public.hitch_4w_sg_self_booking
),

  bilocation as (
    select code1 as code
      from user_trust_public.hitch_4w_sg_dax_booking_each_other
    union
    select code2 as code
      from user_trust_public.hitch_4w_sg_dax_booking_each_other
),

  levenshtein as (
    select code
      from user_trust_public.hitch_4w_sg_dax_pax_levenshtein
),

  --distill booking codes from fraud screenings
  combined_fraud_code as (
    select code
      from (
      select code from self_booking
      union
      select code from bilocation
      union
      select code from levenshtein
      )
      group by code
),

  game_in_mth as (
    select booking_sg_singapore.driver_id as driver_id,
        year(booking_sg_singapore.pick_up_time + interval '8' hour) as yr,
        month(booking_sg_singapore.pick_up_time + interval '8' hour) as mth
      from combined_fraud_code
        inner join grab_hitch.booking_sg_singapore on booking_sg_singapore.code = combined_fraud_code.code
      group by booking_sg_singapore.driver_id,
              year(booking_sg_singapore.pick_up_time + interval '8' hour),
              month(booking_sg_singapore.pick_up_time + interval '8' hour)
),

  multiple_signups as (
  select nric, count(*) as number_account
    from temptables.hitch_supplementary_id_mzi
    where length(nric) >= 9
    group by nric
),

  driver_nric as (
  select hitch_supplementary_id_mzi.driver_id as driver_id, hitch_supplementary_id_mzi.nric, coalesce(multiple_signups.number_account,1) as number_account
    from temptables.hitch_supplementary_id_mzi
      left join multiple_signups on multiple_signups.nric = hitch_supplementary_id_mzi.nric
),

  rides_done_with_referrer as (
  select campaign_referrals.referee_id, count(booking_sg_singapore.code) as rides_with_referrer
    from grab_hitch.booking_sg_singapore
      inner join campaign_referrals on campaign_referrals.referee_id = booking_sg_singapore.driver_id and campaign_referrals.referrer_id = booking_sg_singapore.passenger_id
    where booking_sg_singapore.state_code in (543, 545, 550) and
          booking_sg_singapore.pick_up_time between campaign_referrals.referral_created_time and
            (date_trunc('month', campaign_referrals.referral_created_time) + interval '1' month + interval '6' day)
    group by campaign_referrals.referee_id
),

  referrer_sign_up_code as (
  select distinct c_1.referrer_id, c_2.sign_up_code_used as referrer_sign_up_code_used, c_2.referrer_id as referrer_referrer
    from campaign_referrals as c_1
      inner join campaign_referrals as c_2 on c_1.referrer_id = c_2.referee_id
),

  gc_driver as (
  --gc id and nric
   select identification_card_number,
          id as gc_driver_id,
          created_at_local as gc_created_at_local,
          last_online + interval '8' hour as gc_last_online_local
      from public.drivers
      where country_id = 4
),

  gc_gh_drivers as (
  select gc_driver.gc_driver_id, driver_nric.driver_id as gh_driver_id, gc_driver.gc_created_at_local
    from gc_driver
      inner join driver_nric on driver_nric.nric = gc_driver.identification_card_number
)

select is_DRD.referral_type as referral_type,
        campaign_referrals.referrer_id,
        referrer_details.name as referrer_name,
        referrer_details.phone as referrer_phone,
        referrer_details.email as referrer_email,
        case
          when is_DRD.referral_type = 'DRD' then is_DRD.referrer_create_time
          else referrer_details.create_time_local
        end referrer_create_time,
        case when referrer_gc.gh_driver_id is not null and referrer_gc.gc_created_at_local < campaign_referrals.referral_created_time then True
              else False
        end referrer_is_gc,
        referrer_approved_date.approved_date as referrer_approved_date,
        referrer_sign_up_code.referrer_sign_up_code_used, referrer_sign_up_code.referrer_referrer,
        campaign_referrals.sign_up_code_used,
        campaign_referrals.referee_id, referee_details.name as referee_name, referee_details.phone as referee_phone, referee_details.email as referee_email,
        referee_approved_date.approved_date as referee_approved_date,
        new_dax_rides.rides_done, rides_done_with_referrer.rides_with_referrer, (new_dax_rides.rides_done - coalesce(rides_done_with_referrer.rides_with_referrer,0)) as legit_rides,
        campaign_referrals.referral_created_time, campaign_referrals.sign_up_month,
        case when referee_gc.gh_driver_id is not null and referee_gc.gc_created_at_local < campaign_referrals.referral_created_time then True
              else False
        end referee_is_gc,
        case when referee_gamer.driver_id is not null then 'gamer' else 'pass' end referee_gamer_status,
        case when referrer_gamer.driver_id is not null then 'gamer' else 'pass' end referrer_gamer_status,
        case when referee_nric.nric = referrer_nric.nric then 'self-refer' else 'pass' end self_refer_check,
        case when referee_nric.number_account > 1 then 'multiple accounts' else 'pass' end as mult_acct_check,
        referrer_details.status as referrer_status,
        referee_details.status as referee_status,
        gh_driver.application_status as referee_application_status
  from campaign_referrals
    left join is_DRD on is_DRD.referee_id = campaign_referrals.referee_id
    left join user_details as referee_details on referee_details.user_id = campaign_referrals.referee_id
    left join user_details as referrer_details on referrer_details.user_id = campaign_referrals.referrer_id
    left join new_dax_rides on new_dax_rides.referee_id = campaign_referrals.referee_id
    left join rides_done_with_referrer on rides_done_with_referrer.referee_id = campaign_referrals.referee_id
    left join approved_date as referrer_approved_date on referrer_approved_date.driver_id = campaign_referrals.referrer_id
    left join approved_date as referee_approved_date on referee_approved_date.driver_id = campaign_referrals.referee_id
    left join game_in_mth as referee_gamer on referee_gamer.driver_id = campaign_referrals.referee_id and
                                              referee_gamer.yr = campaign_referrals.yr and
                                              referee_gamer.mth = campaign_referrals.mth
    left join game_in_mth as referrer_gamer on referrer_gamer.driver_id = campaign_referrals.referrer_id and
                                              referrer_gamer.yr = campaign_referrals.yr and
                                              referrer_gamer.mth = campaign_referrals.mth
    left join driver_nric as referee_nric on referee_nric.driver_id = campaign_referrals.referee_id
    left join driver_nric as referrer_nric on referrer_nric.driver_id = campaign_referrals.referrer_id
    left join referrer_sign_up_code on referrer_sign_up_code.referrer_id = campaign_referrals.referrer_id
    left join gc_gh_drivers as referee_gc on referee_gc.gh_driver_id = campaign_referrals.referee_id
    left join gc_gh_drivers as referrer_gc on referrer_gc.gh_driver_id = campaign_referrals.referrer_id
    left join gh_driver on gh_driver.driver_id = campaign_referrals.referee_id
''')

# Commented out IPython magic to ensure Python compatibility.
# %%time
# cursor.execute(query)

df = pd.DataFrame(cursor.fetchall())
df.columns= [i[0] for i in cursor.description]

df.to_csv(file_name, header=True, index=False)

message = 'DRD done\nlink: ' + fld_path + '\nAlt link: ' + shared_folder

sc = SlackClient(slack_TOKEN)

target_id = df_channel[df_channel['channel']=='Superteam']['channel_id'].iloc[0]

sc.api_call(
    "chat.postMessage",
    channel=target_id,
    text=message,
    as_user=True
)
