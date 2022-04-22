from __future__ import print_function
from mailmerge import MailMerge
from datetime import date
import psycopg2
import pandas

template = "...\\template.docx"
output = "...\\EDO_Update_" + str(date.today()) + ".docx"
document = MailMerge(template)

connection = "host=<IP ADDRESS> dbname=<DB NAME> user=<USER> password=<PASSWORD>"
conn = psycopg2.connect(connection)
cur = conn.cursor()

query1 = """Select x.LIC_TYPE, count(*)::text as T1Count from
(SELECT
 CASE
 When lic.License_type in (202, 203, 204) then 'Medical Center'
 When lic.License_type in (205) then 'Medical Cultivation'
 When lic.License_type in (206) then 'Medical Infused-Product Manufacturer-MIPs'
 When lic.License_type in (221) then 'Medical Testing Facility'
 When lic.License_type in (227) then 'Medical Transporter'
 When lic.License_type in (211) then 'Adult Use Store'
 When lic.License_type in (212) then 'Adult Use Cultivation'
 When lic.License_type in (213) then 'Adult Use Infused-Product Manufacturer-MIPs'
 When lic.License_type in (214) then 'Adult Use Testing Facility'
 When lic.License_type in (228) then 'Adult Use Transporter'
 ELSE ''
 END as LIC_TYPE
FROM mylo.t_license lic
WHERE lic.profession_id = 501
  AND lic.license_type IN (202, 203, 204, 205, 206, 211, 212, 213, 214, 221, 227, 228)
  and to_char(lic.issue_date,'yyyy') = to_char(current_date, 'yyyy') ) as x
  GROUP by x.lic_type
  order by x.lic_type"""

query2 = """Select lt.name,
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jan'),'99,999') "JAN",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='feb'),'99,999') "FEB",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='mar'),'99,999') "MAR",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='apr'),'99,999') "APR",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='may'),'99,999') "MAY",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jun'),'99,999') "JUN",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jul'),'99,999') "JUL",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='aug'),'99,999') "AUG",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='sep'),'99,999') "SEP",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='oct'),'99,999') "OCT",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='nov'),'99,999') "NOV",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='dec'),'99,999') "DEC"

from public.licensecounts lc
join mylo.license_type lt on lt.id = lc.license_type
join mylo.license_status ls on ls.id = lc.license_status
join mylo.license_status sls on sls.id = lc.sec_lic_status

where lc.run_date = (date_trunc('MONTH', lc.run_date) + INTERVAL '1 MONTH - 1 day')::date
and date_trunc('YEAR', lc.run_date) = date_trunc('YEAR', current_date)
and lc.sec_lic_status not in (2,6,10,51)
and lc.license_type in (202,203,204,205,206,221,227)
group by lt.name"""

query3 = """Select lt.name,
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jan'),'99,999') "JAN",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='feb'),'99,999') "FEB",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='mar'),'99,999') "MAR",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='apr'),'99,999') "APR",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='may'),'99,999') "MAY",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jun'),'99,999') "JUN",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jul'),'99,999') "JUL",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='aug'),'99,999') "AUG",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='sep'),'99,999') "SEP",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='oct'),'99,999') "OCT",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='nov'),'99,999') "NOV",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='dec'),'99,999') "DEC"
from public.licensecounts lc
join mylo.license_type lt on lt.id = lc.license_type
join mylo.license_status ls on ls.id = lc.license_status
join mylo.license_status sls on sls.id = lc.sec_lic_status
where lc.run_date = (date_trunc('MONTH', lc.run_date) + INTERVAL '1 MONTH - 1 day')::date
and date_trunc('YEAR', lc.run_date) = date_trunc('YEAR', current_date)
and lc.sec_lic_status not in (2,6,10,51)
and lc.license_type in (211,212,213,214,228)
group by lt.name"""

query4 = """SELECT lt.name,
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='jan'),'99,999') "JAN",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='feb'),'99,999') "FEB",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='mar'),'99,999') "MAR",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='apr'),'99,999') "APR",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='may'),'99,999') "MAY",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='jun'),'99,999') "JUN",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='jul'),'99,999') "JUL",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='aug'),'99,999') "AUG",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='sep'),'99,999') "SEP",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='oct'),'99,999') "OCT",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='nov'),'99,999') "NOV",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='dec'),'99,999') "DEC"
FROM mylo.t_license lic
join mylo.license_type lt on lt.id = lic.license_type
WHERE lic.profession_id = 501
  AND lic.license_type in (251,215)
  and lic.issue_date < date_trunc('month',current_date)::date
  and to_char(lic.issue_date,'yyyy')= to_char(current_date, 'yyyy')
  group by lt.name"""

query5 = """SELECT 'Employee',
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='jan'),'99,999') "JAN",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='feb'),'99,999') "FEB",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='mar'),'99,999') "MAR",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='apr'),'99,999') "APR",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='may'),'99,999') "MAY",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='jun'),'99,999') "JUN",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='jul'),'99,999') "JUL",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='aug'),'99,999') "AUG",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='sep'),'99,999') "SEP",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='oct'),'99,999') "OCT",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='nov'),'99,999') "NOV",
to_char(count(lic.license_no) filter(where to_char(lic.issue_date, 'mon')='dec'),'99,999') "DEC"
FROM mylo.t_license lic
WHERE lic.profession_id = 501
  AND lic.license_type  in (207,208,254)
  and lic.issue_date < date_trunc('month',current_date)::date
  and to_char(lic.issue_date,'yyyy')= to_char(current_date, 'yyyy')"""

query6 = """SELECT lt.name,
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='jan'),'99,999') "JAN",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='feb'),'99,999') "FEB",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='mar'),'99,999') "MAR",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='apr'),'99,999') "APR",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='may'),'99,999') "MAY",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='jun'),'99,999') "JUN",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='jul'),'99,999') "JUL",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='aug'),'99,999') "AUG",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='sep'),'99,999') "SEP",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='oct'),'99,999') "OCT",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='nov'),'99,999') "NOV",
to_char(count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='dec'),'99,999') "DEC"
FROM mylo.t_license lic
join mylo.license_type lt on lt.id = lic.license_type
WHERE lic.profession_id = 501
  AND lic.license_type  in (207,208,254)
  and lic.date_last_renewal < date_trunc('month',current_date)::date
  and to_char(lic.date_last_renewal,'yyyy')= to_char(current_date, 'yyyy')
  group by lt.name"""

query7 = """SELECT lt.name,
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='jan'),'99,999') "JAN",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='feb'),'99,999') "FEB",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='mar'),'99,999') "MAR",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='apr'),'99,999') "APR",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='may'),'99,999') "MAY",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='jun'),'99,999') "JUN",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='jul'),'99,999') "JUL",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='aug'),'99,999') "AUG",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='sep'),'99,999') "SEP",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='oct'),'99,999') "OCT",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='nov'),'99,999') "NOV",
to_char(count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='dec'),'99,999') "DEC"
FROM mylo.t_license lic
join mylo.license_type lt on lt.id = lic.license_type
WHERE lic.profession_id = 501
  AND lic.license_type  in (207,208,254)
  AND (lic.date_last_renewal < lic.expiration_date or lic.date_last_renewal is null)
  and lic.expiration_date between date_trunc('year',current_date) AND date_trunc('month',current_date) - interval '1 day'
  group by lt.name"""

query8 ="""With r as
(SELECT 'renew',
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='jan') "JAN",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='feb') "FEB",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='mar') "MAR",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='apr') "APR",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='may') "MAY",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='jun') "JUN",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='jul') "JUL",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='aug') "AUG",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='sep') "SEP",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='oct') "OCT",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='nov') "NOV",
count(lic.license_no) filter(where to_char(lic.date_last_renewal, 'mon')='dec') "DEC"
FROM mylo.t_license lic
join mylo.license_type lt on lt.id = lic.license_type
WHERE lic.profession_id = 501
  AND lic.license_type  in (207,208,254)
  and lic.date_last_renewal < date_trunc('month',current_date)::date
  and to_char(lic.date_last_renewal,'yyyy')= to_char(current_date, 'yyyy'))
  ,

x as
(SELECT 'expired',
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='jan') "JAN",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='feb') "FEB",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='mar') "MAR",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='apr') "APR",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='may') "MAY",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='jun') "JUN",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='jul') "JUL",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='aug') "AUG",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='sep') "SEP",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='oct') "OCT",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='nov') "NOV",
count(lic.license_no) filter(where to_char(lic.expiration_date, 'mon')='dec') "DEC"
FROM mylo.t_license lic
join mylo.license_type lt on lt.id = lic.license_type
WHERE lic.profession_id = 501
  AND lic.license_type  in (207,208,254)
  AND (lic.date_last_renewal < lic.expiration_date or lic.date_last_renewal is null)
  and to_char(lic.expiration_date,'yyyy')= to_char(current_date, 'yyyy')
  and lic.expiration_date < date_trunc('month',current_date)::date)

  Select ' ',
  case when x."JAN" + r."JAN" = 0 then NULL else to_char(100*(r."JAN"/(x."JAN" + r."JAN")::float),'999D99%') end as "JAN",
  case when x."FEB" + r."FEB" = 0 then NULL else to_char(100*(r."FEB"/(x."FEB" + r."FEB")::float),'999D99%') end as "FEB",
  case when x."MAR" + r."MAR" = 0 then NULL else to_char(100*(r."MAR"/(x."MAR" + r."MAR")::float),'999D99%') end as "MAR",
  case when x."APR" + r."APR" = 0 then NULL else to_char(100*(r."APR"/(x."APR" + r."APR")::float),'999D99%') end as "APR",
  case when x."MAY" + r."MAY" = 0 then NULL else to_char(100*(r."MAY"/(x."MAY" + r."MAY")::float),'999D99%') end as "MAY",
  case when x."JUN" + r."JUN" = 0 then NULL else to_char(100*(r."JUN"/(x."JUN" + r."JUN")::float),'999D99%') end as "JUN",
  case when x."JUL" + r."JUL" = 0 then NULL else to_char(100*(r."JUL"/(x."JUL" + r."JUL")::float),'999D99%') end as "JUL",
  case when x."AUG" + r."AUG" = 0 then NULL else to_char(100*(r."AUG"/(x."AUG" + r."AUG")::float),'999D99%') end as "AUG",
  case when x."SEP" + r."SEP" = 0 then NULL else to_char(100*(r."SEP"/(x."SEP" + r."SEP")::float),'999D99%') end as "SEP",
  case when x."OCT" + r."OCT" = 0 then NULL else to_char(100*(r."OCT"/(x."OCT" + r."OCT")::float),'999D99%') end as "OCT",
  case when x."NOV" + r."NOV" = 0 then NULL else to_char(100*(r."NOV"/(x."NOV" + r."NOV")::float),'999D99%') end as "NOV",
  case when x."DEC" + r."DEC" = 0 then NULL else to_char(100*(r."DEC"/(x."DEC" + r."DEC")::float),'999D99%') end as "DEC"
  from r,x"""

query9 ="""Select ' ',
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jan'),'99,999') "JAN",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='feb'),'99,999') "FEB",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='mar'),'99,999') "MAR",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='apr'),'99,999') "APR",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='may'),'99,999') "MAY",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jun'),'99,999') "JUN",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='jul'),'99,999') "JUL",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='aug'),'99,999') "AUG",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='sep'),'99,999') "SEP",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='oct'),'99,999') "OCT",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='nov'),'99,999') "NOV",
to_char(sum(lc.count) filter(where to_char(lc.run_date, 'mon')='dec'),'99,999') "DEC"
from public.licensecounts lc
join mylo.license_type lt on lt.id = lc.license_type
join mylo.license_status ls on ls.id = lc.license_status
join mylo.license_status sls on sls.id = lc.sec_lic_status

where lc.run_date = (date_trunc('MONTH', lc.run_date) + INTERVAL '1 MONTH - 1 day')::date
and to_char(lc.run_date,'yyyy') = to_char(current_date, 'yyyy')
and lc.sec_lic_status not in (2,6,10,51)
and lc.license_type in (207,208,254)"""

query10 = """Select x.type INV_TYPE, to_char(count(*),'999,999') from (
Select distinct
i.investigation_no number, istat.description status, i.date_entered date,
case when i.investigation_type in (5010,5011) then 'Business Background Investigation'
	when i.investigation_type in (5180) then 'Change of Location Investigation'
	when i.investigation_type in (5030) then 'Change of Ownership Investigation'
	when i.investigation_type in (5200) then 'Change of Trade Name Investigation'
	when i.investigation_type in (5077,5073,5072,5074,5075,5070,5012,5071,5076) then 'Individual Background Investigation'
	when i.investigation_type in (5190) then 'Modification of Premises Investigation'
	when i.investigation_type in (5025) then 'Non-Qualified Sales Check Investigation'
	when i.investigation_type in (5051,
5233,5040,5232,5050,5181,5020,5023,5064,5029,5033,5034,5054,5024,5084,5027,5028,5060,5234,5021,
5053,5080,5081,5082,5191,5055,5013,5015,5083,5061,5085,5086,5066,5056,5044,5052,5019,5063,5017,
5016,5018,5110,5220,5065) then 'Regulatory and Criminal Investigation'
	when i.investigation_type in (5210) then 'Renewal Investigation'
	when i.investigation_type in (5022) then 'Targeted Compliance Inspection'
	else null end as TYPE
from mylo.t_mlo_investigation i
LEFT outer join mylo.t_mlo_inv_participant ip on i.investigation_id = ip.investigation_id
LEFT outer JOIN mylo.c_mlo_investigation_status istat ON i.inv_status = istat.inv_status_id
where i.profession_id = 501
AND to_char(i.date_entered,'yyyy') = to_char(current_date, 'yyyy')
and to_char(i.date_entered,'mon') <> to_char(current_date, 'mon')

AND i.investigation_no not like '$INV%'
AND i.investigation_type not in (5032,5310,5150,5031,5160,5042,5041,5043,5026,5062,5045,5230,5231,5161,5014,5170,5100,5105)

UNION ALL

Select distinct
c.complaint_no number, cs.description status, c.date_entered,
case when c.complaint_type in
	(5091,5040,5090,5053,5071,5062,5010,5080,5026,5033,5074,5072,5082,5051,5092,5093,5054,5081,
	 5055,5052,5061,5064,5065,5066,5070,5011,5050,5060,5032,5031,5030,5021,5022,5023,5025,5020,5024,
	 5075,5012,5063,5083,5084,5013,5073) then 'Regulatory and Criminal Investigation'
	else null end as TYPE
from mylo.t_mlo_complaint c
LEFT outer join mylo.v_complaint_complainant cc on c.complaint_id = cc.complaint_id
LEFT outer JOIN mylo.c_mlo_source s ON c.source = s.source_id
LEFT outer JOIN mylo.c_mlo_case_status cs ON c.status = cs.case_status_id
where c.profession_id = 501
AND to_char(c.date_entered,'yyyy') = to_char(current_date, 'yyyy')
and to_char(c.date_entered,'mon') <> to_char(current_date, 'mon')
) x
where x.type is not null
group by x.type"""

cur.execute(query1)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["NewBiz","T1Count"]
table1 = df.to_dict(orient="records")

cur.execute(query2)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["BizType","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table2 = df.to_dict(orient="records")

cur.execute(query3)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["BizType","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table3 = df.to_dict(orient="records")

cur.execute(query4)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["OEType","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table4 = df.to_dict(orient="records")

cur.execute(query5)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["LicType","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table5 = df.to_dict(orient="records")

cur.execute(query6)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["LicType","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table6 = df.to_dict(orient="records")

cur.execute(query7)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["LicType","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table7 = df.to_dict(orient="records")

cur.execute(query8)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["LicRenewPercent","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table8 = df.to_dict(orient="records")

cur.execute(query9)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["LicTypeTotal","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
table9 = df.to_dict(orient="records")

cur.execute(query10)
r = cur.fetchall()
df = pandas.DataFrame(r)
df.columns =["InvType","T10Count"]
table10 = df.to_dict(orient="records")


document.merge_rows('NewBiz', table1)
document.merge_rows('BizType', table2)
document.merge_rows('BizType', table3)
document.merge_rows('OEType', table4)
document.merge_rows('LicType', table5)
document.merge_rows('LicType', table6)
document.merge_rows('LicType', table7)
document.merge_rows('LicRenewPercent', table8)
document.merge_rows('LicTypeTotal', table9)
document.merge_rows('InvType', table10)
document.write(output)
