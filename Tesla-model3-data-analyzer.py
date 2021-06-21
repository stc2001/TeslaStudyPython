import linecache
import random
import matplotlib.pyplot as plt
import pymysql

csv_file_path = '/home/gexingdeyun/桌面/tesla.csv'
csv_file_name = 'tesla.csv'

service_list = {1: "求最大值", 2: "求最小值", 3: "请求指定的目录全部的数据", 4: "求平均值", 5: "绘制线图", 6: "绘制散点图", 7: "导入*.csv数据表" ,8: "搜索&选择数据库"
    ,9:"搜索数据库下的表单"}


# noinspection PyRedeclaration,PyGlobalUndefined

def linecount():
    global countline
    thefile = open(csv_file_path)
    countline = 0
    while True:
        buffer = thefile.read(8192 * 1024)
        if not buffer:
            break
        countline += buffer.count('\n')
    thefile.close()


content = {1: "`Speed (MPH)`",
           2: "`Brake Pressure (bar)`",
           3: "`Elapsed Time (ms)`",
           4: "`Latitude (decimal)`",
           5: "`Longitude  (decimal)`",
           6: "`Battery Temp (%)`",
           7: "`Rear Inverter Temp (%)`",
           8: "`Power Level (KW)`",
           9: "`State of Charge (%)`",
           10: "`Throttle Position (%)`",
           11: "`Lateral Acceleration (m/s^2)`",
           12: "`Longitudinal Acceleration (m/s^2)`"}

content_interface = ["1:速度(英里每小时)",
                     "2:刹车压力(bar)",
                     "3：运行时间(ms)",
                     "4：纬度(小数)",
                     "5：经度(小数)",
                     "6：电池温度(百分比）",
                     "7：后逆变器温度(百分比)",
                     "8：电源水平（KW）",
                     "9：电池电量剩余水平(百分比)",
                     "10：推力(百分比)",
                     "11.横向加速度(米/秒^2)",
                     "12.经度方向加速度(米/秒^2)"]


def generate_uid(topic, size=16):
    characters = 'abcdefghijklmnopqrstuvwxyz$'
    choices = [random.choice(characters) for _x in range(size)]
    return topic.join(choices)


def import_csv(csv_file_path, table_name, database='Tesla'):
    # 打开csv文件
    file = open(csv_file_path, 'r', encoding='utf-8')

    # 读取csv文件第一行字段名，创建表
    reader = file.readline()
    # print(reader)
    b = reader.split(',')
    # print(b)
    colum = ''

    for a in b:
        a = a.strip("\n")
        colum = str(colum + "`" + a + "`" + ' varchar(255),')
        # print(colum)
    colum = colum[:-1]
    print(colum)
    # 编写sql，create_sql负责创建表，data_sql负责导入数据
    create_sql = 'create table if not exists ' + table_name + '  ' + '(' + colum + ')' + ' DEFAULT CHARSET=utf8'
    print(create_sql)

    cursor.execute('use %s' % database)
    print("set names utf-8")
    # 设置编码格式
    cursor.execute('SET NAMES utf8;')
    print("set character_set_connection = utf8")
    cursor.execute('SET character_set_connection=utf8;')
    # 执行create_sql，创建表
    print("creating......")
    cursor.execute(create_sql)
    # 执行data_sql，导入数据
    print("importing......")

    linecount()
    print(countline)
    # 读取csv文件第一行字段名，创建表

    file = open(csv_file_path, 'r', encoding='utf-8')
    reader = file.readline()
    # print(reader)
    b = reader.split(',')
    # print(b)
    colum = ''

    for a in b:
        a = a.strip("\n")
        colum = str(colum + "`" + a + "`" + ',')

    colum = colum[:-1]
    print(colum)

    for i in range(countline - 1):
        reader_data = linecache.getline(csv_file_path, i + 2)
        reader_data = reader_data.strip("\n")
        reader_data = str(reader_data.split(','))[1:-1]
        print(reader_data)
        cmd = str('INSERT INTO ' + table_name + ' (' + colum + ') ' + ' VALUES' + ' (' + reader_data + ');')
        print('command:' + cmd)
        print("正在导入第%s条数据......" % i)
        cursor.execute(cmd)

    print("commiting......")
    db.commit()
    print("已完成")


# noinspection PyBroadException
def database_connect():
    # noinspection PyGlobalUndefined
    global db, cursor

    print("正在连接Tesla车辆数据记录数据库......")
    try:
        db = pymysql.connect(host="localhost",
                             user="root",
                             password="570621",
                             db="Tesla",
                             charset="utf8")
        print("数据库连接成功")

    except:

        print("数据库连接失败，程序被迫中止")

    cursor = db.cursor()
    print("您的车型是：“[国产]Tesla-model3”")


def database_disconnect():
    # 关闭数据库连接
    print("正在断开连接数据库......")
    db.close()
    print("已与数据库断开连接,程序已终止")


def max_value():
    cmd = list("SELECT max() from `tesla-model3`")
    print("请选择所求的最大值项目：")
    print(content_interface)
    a = int(input())
    print("正在运行......")
    cmd.insert(11, content[a])
    cmd = ''.join(cmd)
    print(cmd)
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("以下是请求结果")
    print(data)


def min_value():
    cmd = list("SELECT min() from `tesla-model3`")
    print("请选择所求的最小值项目：")
    print(content_interface)
    a = int(input())
    print("正在运行......")
    cmd.insert(11, content[a])
    cmd = ''.join(cmd)
    print(cmd)
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("以下是请求结果")
    print(data)


def avg_value():
    cmd = list("SELECT avg() from `tesla-model3`")
    print("请选择所求的平均值项目：")
    print(content_interface)
    a = int(input())
    print("正在运行......")
    cmd.insert(11, content[a])
    cmd = ''.join(cmd)
    print(cmd)
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("以下是请求结果")
    print(data)


# noinspection PyGlobalUndefined,PyPep8Naming
def value_all():
    # global data_value_all
    dataValueAll: list[float] = []
    cmd = list("SELECT  from `tesla-model3`")
    print("请选择所求的项目：")
    print(content_interface)
    a = int(input())
    print("正在运行......")
    cmd.insert(7, content[a])
    cmd = ''.join(cmd)
    print(cmd)
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("是否逐行打印，按‘y’并回车以同意，按其它任意键并回车以跳过逐行打印")
    type_in = input()
    if type_in == "y":
        print("以下是请求结果")

        for i in range(len(data)):
            data_list = list(data[i])
            data_list = str(data_list)
            data_list = data_list[1:]
            data_list = data_list[:-1]
            data_list = float(data_list)
            dataValueAll.append(data_list)
            print(data_list)
            print(i)
    else:
        for i in range(len(data)):
            data_list = list(data[i])
            data_list = str(data_list)
            data_list = data_list[1:-1]
            data_list = float(data_list)
            dataValueAll.append(data_list)

    print("全部数据如下：")
    print(dataValueAll)


def command():
    print("请选择数据分析项目：(若要终止程序，请按Ctrl+C)")
    print(service_list)
    print("请输入数字，例如：'1,2,3...'")
    input_string = input()
    if input_string == "1":
        max_value()
    if input_string == "2":
        min_value()
    if input_string == "3":
        value_all()
    if input_string == "4":
        avg_value()
    if input_string == "5":
        data_plot()
    if input_string == "6":
        data_scatter_plt()
    if input_string == "7":
        # csv_file_path = 'tesla.csv'
        # csv_file_name = 'tesla.csv'
        uid = generate_uid('', size=8)
        table_name = str(uid)
        import_csv(csv_file_path, table_name, database='Tesla')
    if input_string == "8":
        search_database()
    if input_string == "9":
        search_table()

# noinspection PyGlobalUndefined
def data_plot():
    global data_value_all, data_run_time_all
    atty = []
    b = 0
    data_value_all = []
    data_run_time_all = []
    cmd = list("SELECT  from `tesla-model3`")
    print("请选择所求的项目：")
    print(content_interface)
    a = int(input())
    print("正在运行......")
    cmd.insert(7, content[a])
    cmd = ''.join(cmd)
    print(cmd)
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("以下是请求结果")
    for i in range(len(data)):
        b = b + 1
        atty.append(b)

    for i in range(len(data)):
        data_list = list(data[i])
        data_list = str(data_list)
        data_list = data_list[1:]
        data_list = data_list[:-1]
        data_list = float(data_list)
        data_value_all.append(data_list)

    print("全部数据如下：")

    print(data_value_all)
    print("正在绘图......")
    plt.plot(atty, data_value_all)
    print("正在显示......")
    plt.show()
    print("已关闭绘图界面")


# noinspection PyGlobalUndefined
def data_scatter_plt():
    global data_value_all, data_run_time_all
    atty = []
    b = 0
    data_value_all = []
    data_run_time_all = []
    cmd = list("SELECT  from `tesla-model3`")
    print("请选择所求的项目：")
    print(content_interface)
    a = int(input())
    print("正在运行......")
    cmd.insert(7, content[a])
    cmd = ''.join(cmd)
    print(cmd)
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("以下是请求结果")
    for i in range(len(data)):
        b = b + 1
        atty.append(b)

    for i in range(len(data)):
        data_list = list(data[i])
        data_list = str(data_list)
        data_list = data_list[1:]
        data_list = data_list[:-1]
        data_list = float(data_list)
        data_value_all.append(data_list)

    print("全部数据如下：")

    print(data_value_all)
    print("正在绘图......")
    plt.scatter(atty, data_value_all)
    print("正在显示......")
    plt.show()
    print("已关闭绘图界面")


def search_database():
    print('正在搜索数据库......')
    cmd='show databases;'
    cursor.execute(cmd)
    data=cursor.fetchall()
    print("以下是检索到的数据库：")
    print(data)
    cmd=input("请输入数据库名字以访问：")
    cmd="use "+"`"+cmd+"`"+";"
    print(cmd)
    print("正在执行......")
    try:
        cursor.execute(cmd)
        data = cursor.fetchall()
        print(data)
        print()
        print("已完成")
    except:
        print("未找到数据库，请核实您输入的内容")

def search_table():
    print('正在搜索数据库表单......')
    cmd = 'show tables;'
    cursor.execute(cmd)
    data = cursor.fetchall()
    print("以下是检索到的数据库表单：")
    print(data)
    print("请选择服务项目：")
    print("1:删除表单"
          "2:删除某一表单下的列数据"
          "3:删除某一表单下的元数据"
          "4:修改指定的column下的元组")
    cmd = input()
    if cmd == "1":
        table_name=""
        table_name = input("请输入你要删除的表单")
        cmd = "drop table " + "`" + table_name + "`" + ";"
        print("正在运行......")
        print(cmd)
        cursor.execute(cmd)
        data = cursor.fetchall()
        print(data)
        print("已完成")
    if cmd == "2" :
        colum_name = ''
        table_name = input("请输入你要操作的表单")
        print("以下是这个表单下的全部列：")
        cmd = "SHOW FULL COLUMNs FROM " + "`" + table_name + "`" + ";"
        cursor.execute(cmd)
        data=cursor.fetchall()
        print(data)
        print("请选择要删除的列：")
        colum_name=input()
        cmd = "alter table " + "`" + table_name + "`" + "drop column "+  "`" +colum_name+ "`" +";"
        print(cmd)
        print("running......")
        cursor.execute(cmd)
        print("正在确认......")
        db.commit()
        data = cursor.fetchall()
        print(data)
        print("complete.")
    if cmd == "3" :
        del_focus = ''
        table_name = input("请输入你要操作的表单")
        print("以下是这个表单下的全部列：")
        cmd = "SHOW FULL COLUMNs FROM " + "`" + table_name + "`" + ";"
        cursor.execute(cmd)
        data = cursor.fetchall()
        print(data)
        print("请选择要操作的column：")
        colum_name = input()
        print("请输入你要删除的元组对象下的你选择的colum的数据")
        del_focus = input()
        cmd = "delete from " + "`"+ table_name +"` "  +"where " +"`"+colum_name + "`" + " = "+del_focus + " ;"
        print(cmd)
        print("正在执行......")
        cursor.execute(cmd)
        print("正在确认......")
        db.commit()
        data = cursor.fetchall()
        print(data)
        print("完成.")
    if cmd == "4" :
        del_focus = ''
        change_data=''
        change_data_identity=''
        table_name = input("请输入你要操作的表单")
        print("以下是这个表单下的全部列：")
        cmd = "SHOW FULL COLUMNs FROM " + "`" + table_name + "`" + ";"
        cursor.execute(cmd)
        data = cursor.fetchall()
        print(data)
        print("请选择要操作的column：")
        colum_name = input()
        print("请输入你要操作的colum下的用来定位的colum：")
        del_focus = input()
        print("请输入你要修改的内容：")
        change_data=input()
        print("请输入你要修改的内容所在的行里面的你刚刚指定的用来定位的colum下的元组数据：")
        change_data_identity=input()
        cmd = "update "+ "`"+table_name+ "` "+"set "+ "`"+colum_name+ "` "+"= "+change_data+" where "+ "`"+del_focus+ "` "+" = "+change_data_identity+" ;"
        print("这是根据您的请求生成的指令："+cmd)
        print("正在运行......")
        cursor.execute(cmd)
        print("正在确认......")
        db.commit()
        data=cursor.fetchall()
        print(data)
        print("已完成.")



database_connect()

while 1:
    try:

        command()

    except KeyboardInterrupt:
        database_disconnect()
        break

'''
SELECT  `tesla-model3`.`Speed (MPH)`,`xxnnhabz`.`Speed (MPH)` from `tesla-model3` , `xxnnhabz` limit 10;
#多表查询(不同表单下的相同车辆参数对比)
SELECT `Elapsed Time (ms)` ,GROUP_CONCAT(`Brake Pressure (bar)`) from `tesla-model3`  group by `Elapsed Time (ms)` limit 10;
#分组查询，由于CAN总线的传输数据的时间之间的间隔各不相同，有的时候同一运行时有多个数据，分组查询能够将同一个时间点的重复数据放在一起，实现数据的整理与清洗，有效解决数据混乱的问题。
SELECT `Elapsed Time (ms)`,`Speed (MPH)`,`Steering Angle (deg)` from `tesla-model3` where `Speed (MPH)` in (select `Speed (MPH)` from `xxnnhabz` where `Steering Angle (deg)`='0') limit 10
#嵌套查询(嵌套查询不建议在这种场景下的数据库使用案例下使用.)
select `Latitude (decimal)` ,`Longitude (decimal)` from  `tesla-model3` union  select `Latitude (decimal)` ,`Longitude (decimal)`  from `xxnnhabz` limit 10 ;
#集合查询(集合查询不建议在这种场景下的数据库使用案例下使用.)
'''