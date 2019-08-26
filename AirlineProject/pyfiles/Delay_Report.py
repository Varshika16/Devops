#!/usr/bin/python
import sys
import commands
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
user_home = os.path.expanduser('~')
user_dir = user_home+"/Reports"
if not os.path.exists(user_dir):
    os.mkdir(user_dir)

for line in sys.stdin:    
    tbl = line.split(".")
    report = tbl[1].strip()+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    csv_file = user_home+"/AirlineProject/csv/"+report+".csv"
    query = "select * from "+line+" order by 1 asc"
    result_string= 'beeline -u "jdbc:hive2://" -n "tsipl0514" -p "tsipl0514" --outputformat=csv2 -e "'+query+'" > '+csv_file
    status, output = commands.getstatusoutput(result_string)
    if status == 0: 
        df = pd.read_csv(csv_file)
        
        df = df.astype(float)
        
        df = df.sort(df.columns[0])
        
        writer = pd.ExcelWriter(user_dir+"/"+report+".xlsx", engine='xlsxwriter')
        df.to_excel(writer, index = False ,sheet_name=tbl[0].strip(),cols=df.columns)

        workbook = writer.book
        worksheet = writer.sheets[tbl[0].strip()]

        chart = workbook.add_chart({'type': 'column'})
        letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P']
        tab_name = "Sheet1"
        for n in range(0, len(df.columns)-1):
            chart.add_series({
                'values': '='+tbl[0].strip()+'!$B$2:$B$'+ str(len(df.index)+1),
                'gap':        50,
                'border': {'color': 'black'},
                'fill': {'color': '#189aaa'},
                'width':500

            })



        chart.set_plotarea({
            'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']},
            'fg_color': 'red'
        })
        chart1 = workbook.add_chart({'type': 'line'})
        for n in range(0, len(df.columns)-1):
            chart1.add_series({
                'values': '='+tbl[0].strip()+'!$B$2:$B$'+ str(len(df.index)+1),
                'gap':        50,
                'border': {'color': 'red'},
                'fill': {'color': '#b76c5c'},
                'width':500

            })

        chart1.set_plotarea({
            'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']},
            'fg_color': 'red'
        })

        chart1.set_size({'width': 500, 'height': 300})

        chart.set_size({'width': 500, 'height': 300})
        chart.set_x_axis({'name': df.columns[0].split(".")[1]})
        chart.set_y_axis({'name': df.columns[1].split(".")[1]})
        worksheet.insert_chart('E2', chart)
        
        chart1.set_x_axis({'name': df.columns[0].split(".")[1]})
        chart1.set_y_axis({'name': df.columns[1].split(".")[1]})
        worksheet.insert_chart('M2', chart1)

        writer.save()

    else:
        print("Error encountered while executing HiveQL queries.")
        break
