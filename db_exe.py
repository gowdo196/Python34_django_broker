# -*- coding: utf8 -*-
import sys, os
import time
import datetime
from datetime import date
import threading
import logging, logging.handlers
import psycopg2
import psycopg2.extras
#import configparser
import os
import fileinput
import socket
sys.path.append('C:\\Python34\\mysite\\broker')
import db_test

global login_seq
global act_timer

login_seq = 0
act_timer = False
record_mode = "0"#file = 0, postgrasql = 1
ZF_socket_accounting = ('192.168.150.126',9112)#中菲正式機(帳務)socket (IP,port)

def broker_index_results():
    if record_mode == "0":
        results = db_test.read_information_from_temp(1)# 0: tb_vip_status, 1:tb_vip_relation
    elif record_mode == "1":
        sqlcmd = "select sn,ip,version,area,max_link,pro,act,last_update from dfh.tb_vip_relation ORDER BY sn;"
        results = db_test.DB_select_connect(sqlcmd)
    return results
def broker_index_currlink_result():
    if record_mode == "0":
        currlink_result = db_test.read_information_from_temp(0)
    elif record_mode == "1":
        currlink_result = db_test.DB_select_connect("select ip,curr_link from dfh.tb_vip_status;")
    return currlink_result
def socket_connect(id_,pwd_):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(ZF_socket_accounting)
        #sock.connect(('192.168.210.75',9112))#中菲外部IP
        #sock.connect(('203.69.48.26',9112))
        #sock.connect(('192.168.210.75',9112))#中菲大昌LOCAL
    except:
        DB_log('ERROR','socket_connect',' connect to'+str(('203.69.245.142',9112))+' failed')
    length = 71
    seq = 1
    length1 = int(length / 256)
    length2 = length % 256

    now_time_str = str(datetime.datetime.now())[11:13]+str(datetime.datetime.now())[14:16]+str(datetime.datetime.now())[17:19]+str(datetime.datetime.now())[20:23]
    
    head_start = chr(0x11)
    head = now_time_str+str(seq).rjust(10, '0')+'000'+'AP1'+'       '+'               '
    seq = seq + 1
    body_length1 = chr(int(hex(length1),16))
    body_length2 = chr(int(hex(length2),16))
    body = id_+'       '+'       '+'0'+'A'+pwd_.ljust(20,' ')+'192.168.210.75           '
    end = chr(0x0a)

    sock.send((head_start+head+body_length1+body_length2+body+end).encode('ascii'))

    data = sock.recv(1024)
    DB_log('DEBUG','socket_connect','get recv data = '+str(data))
    return data
def login(id_,pwd_,ver_,area_,ip_):
    """if id_ != 'slogin':
        if(len(find_cert(id_,pwd_)) > 0):
            logout(find_cert(id_,pwd_))"""
    login_dic = {}
    login_dic.update({'login_account':id_})
    login_dic.update({'login_pwd':pwd_})

    login_cert = create_cert()
    login_dic.update({'login_cert':login_cert})
    #20160818 ckernel&dkernel new login mode
    if ver_.split(',')[0].strip() == '':
        #account = login_get_account_by_id_from_HK_DB(id_)
        account = get_competence_by_id(id_)
        if str(account) == '4':
            login_dic.update({'account_competence':account})
        else:
            #login_dic.update({'account_competence':get_competence_by_account(account[0][0],account[0][1]+account[0][2])})
            login_dic.update({'account_competence':'1'})
        login_vip_ip = vip_distribution("Thin"+ver_,area_)
        hk_login_vip_ip = vip_distribution("WinHK"+ver_,area_)
        login_dic.update({'HK_login_vip_ip':hk_login_vip_ip})
        hk_ip_port = hk_login_vip_ip.split(':')
    #20160823 ckernel&dkernel old login mode(pending)
    else:
        login_vip_ip = vip_distribution(ver_,area_)
        hk_ip_port = ['','']
    login_dic.update({'login_vip_ip':login_vip_ip})
    ip_port = login_vip_ip.split(':')

    startDate = datetime.datetime.now()
    expire_date = date(startDate.year +1,startDate.month,startDate.day)

    DB_log('DEBUG','login',db_test.key_exist_in_object('login_cert',login_dic))
    DB_log('DEBUG','login',db_test.key_exist_in_object('login_vip_ip',login_dic))
    DB_log('DEBUG','login',db_test.key_exist_in_object('login_account',login_dic))#20160321 記錄自打送來的帳號
    if record_mode == "0":
        #db_test.write_temp(login_dic,0,'add_cert')
        pass
    elif record_mode == "1":
        db_test.DB_update_connect("insert into dfh.tb_vip_certification (ip,account,pwd,cert)VALUES(\'"+login_vip_ip+"\',\'"+str(id_)+"\',\'"+str(pwd_)+"\',\'"+login_cert+"\')")

    #after add tb_vip_certification 20151229
    #20160120 socket_server_test ok
    dic_result = {}
    if id_ == 'slogin':#20160122 slogin
        dic_result.update({'login_vip_ip':ip_port[0]})
        dic_result.update({'login_vip_port':ip_port[1]})
        dic_result.update({'login_cert':login_cert})
        dic_result.update({'expire_date':str(expire_date).replace('-','')})
        return dic_result
    elif db_test.read_config_list(8)[0] == '0':#beta 0版(歸戶ID不由中菲認證)
        pass
    else:
        data = socket_connect(id_,pwd_)
    #select backend DB to get
    """
    if id_ == 'slogin':
        data = b'\x111112288890000000001000AP10000000               \x005001S6460   8889993\xb4\xfa\xb8\xd52                 IYYA222456789\n'
    else:
        if id_ == 'A123456789':
            data = b'\x111112288260000000001000AP10000000               \x00\xcb004S6460   8888884\xb4\xfa\xb8\xd51                 AYYA123456789S6461   8888883\xb4\xfa\xb8\xd51                 AYYA123456789FF0390006666666\xb4\xfa\xb8\xd5\xb1b\xb8\xb9              AYYA123456789FF0390007777774\xb4\xfa\xb8\xd5\xb1b\xb8\xb9              AYYA123456789\n'
        elif id_ == 'A123456798':
            data = b'\x111112288570000000002000AP10000000               \x00g002S6460   7777776\xb4\xfa\xb8\xd54                 AYYA123456798FF0390005555558\xb4\xfa\xb8\xd5\xb1b\xb8\xb9              AYYA123456798\n'
        elif id_ == 'M123456789':
            data = b'\x111112288890000000003000AP10000000               \x005001S6460   8889993\xb4\xfa\xb8\xd52                 AYYM123456789\n'
    """
    # 20160113 for login

    re_list = []
    if db_test.read_config_list(8)[0] == '0':
        futures_account_list = []
        stock_account_list = []
        TW_futures_account_list = []
        TW_stock_account_list = []
        #stock account
        stock_account_result = db_test.DB_select_connect("select cseq,id,account_mode from public.tb_customer where id = '"+id_+"' and account_mode = 1")
        if len(stock_account_result) > 0:
            for item in stock_account_result:
                TW_stock_account_list.append(item[0])
            dic_result.update({'TW_stock_account_list':TW_stock_account_list})
        else:#20170309 新增 若DB無此身分證,預設給台股VIP權限
            dic_result.update({'TW_stock_account_list':["None"]})

        #Future account 處理期貨帳號
        future_account_result = db_test.DB_select_connect("select cseq,id,account_mode from public.tb_customer where id = '"+id_+"' and account_mode = 2")
        if len(future_account_result) > 0:
            for item in future_account_result:
                TW_futures_account_list.append(item[0])
            dic_result.update({'TW_futures_account_list':TW_futures_account_list})
        else:#20170309 新增 若DB無此身分證,預設給台股VIP權限
            dic_result.update({'TW_futures_account_list':["None"]})

        #if len(future_account_result) == 0 and len(stock_account_result) == 0:
            #dic_result.update({'errorcode':'0000004'})
            #dic_result.update({'error_msg':'查無帳務資料'})

        dic_result.update({'login_vip_ip':ip_port[0]})
        dic_result.update({'login_vip_port':ip_port[1]})
        dic_result.update({'login_cert':login_cert})
        dic_result.update({'HK_login_vip_ip':hk_ip_port[0]})
        dic_result.update({'HK_login_vip_port':hk_ip_port[1]})
        dic_result.update({'TW_futures_account_list':TW_futures_account_list})
        dic_result.update({'TW_stock_account_list':TW_stock_account_list})
        dic_result.update({'futures_account_list':futures_account_list})
        dic_result.update({'stock_account_list':stock_account_list})
        dic_result.update({'account_competence':db_test.key_exist_in_object('account_competence',login_dic)})
        dic_result.update({'expire_date':str(expire_date).replace('-','')})
        dic_result.update({'order_server_ip':db_test.read_config_list(1)})
    else:
        if int.from_bytes(data[48:50],byteorder='little')>0:
            cut_count = int(data[50:53].decode('big5'))

            for r in range(cut_count):
                re_list.append(data[53+50*r:103+50*r])
                if len(re_list) > 0:
                    futures_account_list = []
                    stock_account_list = []
                    TW_futures_account_list = []
                    TW_stock_account_list = []
                    for item in re_list:
                        if item[37:38].decode('big5') == 'I':
                            if item[0:1].decode('big5') == 'F':
                                TW_futures_account_list.append(item[8:15].decode('big5'))
                            else:# item[0:1].decode('big5') == 'S'
                                TW_stock_account_list.append(item[8:15].decode('big5'))
                        elif item[37:38].decode('big5') == 'O':
                            if item[0:1].decode('big5') == 'F':
                                futures_account_list.append(item[8:15].decode('big5'))
                            else:# item[0:1].decode('big5') == 'S'
                                stock_account_list.append(item[8:15].decode('big5'))
                        else:#item[37:38].decode('big5') == 'A'
                            if item[0:1].decode('big5') == 'F':
                                futures_account_list.append(item[8:15].decode('big5'))
                                TW_futures_account_list.append(item[8:15].decode('big5'))
                            else:# item[0:1].decode('big5') == 'S'
                                stock_account_list.append(item[8:15].decode('big5'))
                                TW_stock_account_list.append(item[8:15].decode('big5'))
            dic_result.update({'login_vip_ip':ip_port[0]})
            dic_result.update({'login_vip_port':ip_port[1]})
            dic_result.update({'login_cert':login_cert})
            dic_result.update({'HK_login_vip_ip':hk_ip_port[0]})
            dic_result.update({'HK_login_vip_port':hk_ip_port[1]})
            #20160823
            dic_result.update({'account_competence':db_test.key_exist_in_object('account_competence',login_dic)})
            dic_result.update({'TW_futures_account_list':TW_futures_account_list})
            dic_result.update({'TW_stock_account_list':TW_stock_account_list})
            dic_result.update({'futures_account_list':futures_account_list})#
            dic_result.update({'stock_account_list':stock_account_list})#
            dic_result.update({'expire_date':str(expire_date).replace('-','')})
            dic_result.update({'order_server_ip':db_test.read_config_list(1)})
        else:
            find_msg = ""
            dic_result.update({'errorcode':data[26:33].decode('big5')})
            for item in db_test.read_config_list(7):
                if item.split(':')[0] == data[26:33].decode('big5'):
                    find_msg = item.split(':')[1]
            dic_result.update({'error_msg':find_msg})
            DB_log('ERROR','login','get errorcode = '+str(data[26:33].decode('big5')))
    return dic_result
#20160818
def login_get_account_by_id_from_HK_DB(id_):
    sqlcmd = "select bhno,cseq,a.confirm from dfh.tb_customer_attribute \
                LEFT JOIN dfh.tb_customerinfo a on bhno = a.branch and cseq = a.cesq\
                where amode = '14' and att = '"+id_+"'"
    result = db_test.HK_DB_select_connect(sqlcmd)
    return result

def get_competence_by_account(bhno_,cseq_):
    sqlcmd = "select account_mode from public.tb_customer where bhno = '"+bhno_+"' and cseq = '"+cseq_+"' and status = '1' order by account_mode desc limit 1"
    result = db_test.DB_select_connect(sqlcmd)
    return result[0][0]

def get_competence_by_id(id_):
    sqlcmd = "select account_mode from public.tb_customer where id = '"+id_+"' and status in ('1','3') order by account_mode desc limit 1"
    result = db_test.DB_select_connect(sqlcmd)
    if len(result) == 0:
        result = [('4',)]#20170802 港股報價權限控制
    return result[0][0]

def logout(cert_):
    have_cert = ""
    have_id = ""
    if record_mode == "0":
        tb_vip_status_list = db_test.read_information_from_temp(0)
        tb_vip_certification_list = db_test.read_information_from_temp(2)#tb_vip_certification
    elif record_mode == "1":
        # select where have some problem WTF??? 20151229
        tb_vip_status_list = db_test.DB_select_connect("select * from dfh.tb_vip_status")
        tb_vip_certification_list = db_test.DB_select_connect("select ip,account,pwd,cert from dfh.tb_vip_certification")
    for r in tb_vip_certification_list:
        if cert_ == r[3]:
            vip_ip = r[0]
            have_id = r[1]
            have_cert = r[3]
    if len(have_cert) > 0:
        if record_mode == "0":
            dic = {}
            dic.update({'login_cert':have_cert})
            #db_test.write_temp(dic,0,'del_cert')
            # curr_link -1
            for s in tb_vip_status_list:
                if vip_ip == s[0]:
                    if int(s[1]) > 0 :
                        curr_link = int(s[1])-1
            dic.update({'txb_ip':vip_ip})
            dic.update({'curr_link':str(curr_link)})
            db_test.write_temp(dic,0,'update_relation')
        elif record_mode == "1":
            db_test.DB_update_connect("delete from dfh.tb_vip_certification where cert = \'"+have_cert+"\'")
        DB_log("DEBUG","logout","id = "+have_id+", cert = "+cert_)
def find_cert(id_,pwd_):
    have_cert = ""
    if record_mode == "0":
        tb_vip_status_list = db_test.read_information_from_temp(0)
        tb_vip_certification_list = db_test.read_information_from_temp(2)#tb_vip_certification
    elif record_mode == "1":
        # select where have some problem WTF??? 20151229
        tb_vip_status_list = db_test.DB_select_connect("select * from dfh.tb_vip_status")
        tb_vip_certification_list = db_test.DB_select_connect("select ip,account,pwd,cert from dfh.tb_vip_certification")
    for r in tb_vip_certification_list:
        if id_ == r[1] and pwd_ == r[2]:
            have_cert = r[3]
    return have_cert

def query_cert(cert_):
    result = ""
    dic_ = {}
    if record_mode == "0":
        tb_vip_certification_list = db_test.read_information_from_temp(2)#tb_vip_certification
    elif record_mode == "1":
        # select where have some problem WTF??? 20151229
        tb_vip_certification_list = db_test.DB_select_connect("select ip,account,pwd,cert from dfh.tb_vip_certification")
    for r in tb_vip_certification_list:
        if cert_ == r[3]:
            result = r[3]
    return result
def check_cert(cert_):
    dic_ = {}
    if record_mode == "0":
        dic_.update({'login_cert':cert_})
        #db_test.write_temp(dic_,1,'check_cert')# 1 : Overwrite relation
        #db_test.write = db_test.read_information_from_temp(2)#tb_vip_certification
    elif record_mode == "1":
        tb_vip_certification_list = db_test.DB_select_connect("select ip,account,pwd,cert,certcheck from dfh.tb_vip_certification")
        for r in tb_vip_certification_list:
            if r[3] == cert_:
                certcheck = int(r[4])+1
                db_test.DB_update_connect("update dfh.tb_vip_certification set certcheck = \'"+str(certcheck)+"\' where cert = "+cert_+";")
def mod_list(dic_,write_file_TF):
    if write_file_TF:
        txb_sn = db_test.key_exist_in_object('txb_sn',dic_)
        txb_ip = db_test.key_exist_in_object('txb_ip',dic_)
        txb_ver = db_test.key_exist_in_object('txb_ver',dic_)
        txb_area = db_test.key_exist_in_object('txb_area',dic_)
        curr_link = db_test.key_exist_in_object('curr_link',dic_)
        txb_max_link = db_test.key_exist_in_object('txb_max_link',dic_)
        if txb_ver.split(',')[0] == 'Thin':
            txb_pro = 'D'
        else:
            txb_pro = 'S'
        dic_.update({'txb_pro':txb_pro})
        #txb_pro = db_test.key_exist_in_object('txb_pro',dic_)
        txb_act = db_test.key_exist_in_object('txb_act',dic_)
        if record_mode == "0":
            db_test.write_temp(dic_,0,'update_relation')# 0 : Overwrite status
            db_test.write_temp(dic_,1,'update_relation')# 1 : Overwrite relation
        elif record_mode == "1":
            sqlcmd = 'update dfh.tb_vip_relation set '
            if len(txb_ver)>0:
                sqlcmd = sqlcmd + 'version = \''+ txb_ver + '\','
            if len(txb_area)>0:
                sqlcmd = sqlcmd + 'area = \''+ txb_area + '\','
            if txb_max_link != "--" and len(txb_max_link)>0:
                sqlcmd = sqlcmd + 'max_link = \''+ txb_max_link + '\','
            if txb_pro != "--" and len(txb_pro)>0:
                sqlcmd = sqlcmd + 'pro = \''+ txb_pro + '\','
            if txb_act != "--" and len(txb_act)>0:
                sqlcmd = sqlcmd + 'act = \''+ txb_act + '\','
            sqlcmd = sqlcmd + 'last_update = timezone(\'CCT\'::text, now()) where sn =\'' + txb_sn+'\';'
            db_test.DB_update_connect(sqlcmd)
        DB_log('DEBUG','mod_list','==== mod_list complete ====')
    if record_mode == "0":
        results = db_test.read_information_from_temp(1)#0: tb_vip_status, 1: tb_vip_relation
    elif record_mode == "1":
        sqlcmd = 'select sn,ip,version,area,max_link,pro,act,last_update from dfh.tb_vip_relation ORDER BY sn;'
        results = db_test.DB_select_connect(sqlcmd)
    return results
def set_config(dic_,write_file_TF):
    if write_file_TF:
        txb_sn = db_test.key_exist_in_object('txb_sn',dic_)
        txb_ip = db_test.key_exist_in_object('txb_ip',dic_)
        txb_ver = db_test.key_exist_in_object('txb_ver',dic_)
        txb_area = db_test.key_exist_in_object('txb_area',dic_)
        curr_link = db_test.key_exist_in_object('curr_link',dic_)
        txb_max_link = db_test.key_exist_in_object('txb_max_link',dic_)
        txb_pro = db_test.key_exist_in_object('txb_pro',dic_)
        txb_act = db_test.key_exist_in_object('txb_act',dic_)

        og_txb_ip = db_test.key_exist_in_object('og_txb_ip',dic_)
        og_txb_ver = db_test.key_exist_in_object('og_txb_ver',dic_)
        og_txb_area = db_test.key_exist_in_object('og_txb_area',dic_)
        if record_mode == "0":# update file
            db_test.write_temp(dic_,0,'set')# 0 : Overwrite status
            db_test.write_temp(dic_,1,'set')# 1 : Overwrite relation
        elif record_mode == "1":# update postgraSQL 
            if len(og_txb_ip)>0:
                sqlcmd="update dfh.tb_vip_relation set ip = \'"+txb_ip+"\',last_update = timezone(\'CCT\'::text, now()) where ip = \'"+og_txb_ip+"\'"
                db_test.DB_update_connect(sqlcmd)
            if len(og_txb_ver)>0:
                sqlcmd="update dfh.tb_vip_relation set version = \'"+txb_ver+"\',last_update = timezone(\'CCT\'::text, now()) where version = \'"+og_txb_ver+"\'"
                db_test.DB_update_connect(sqlcmd)
            if len(og_txb_area)>0:
                sqlcmd="update dfh.tb_vip_relation set area = \'"+txb_area+"\',last_update = timezone(\'CCT\'::text, now()) where area = \'"+og_txb_area+"\'"
                db_test.DB_update_connect(sqlcmd)
        db_test.write_config(dic_)
        DB_log('DEBUG','broker_set_config','==== set_config complete ====')
def add_vip(dic_,write_file_TF):
    if write_file_TF:
        txb_sn = db_test.key_exist_in_object('txb_sn',dic_)
        txb_ip = db_test.key_exist_in_object('txb_ip',dic_)
        txb_ver = db_test.key_exist_in_object('txb_ver',dic_)
        txb_area = db_test.key_exist_in_object('txb_area',dic_)
        curr_link = db_test.key_exist_in_object('curr_link',dic_)
        txb_max_link = db_test.key_exist_in_object('txb_max_link',dic_)
        txb_pro = db_test.key_exist_in_object('txb_pro',dic_)
        txb_act = db_test.key_exist_in_object('txb_act',dic_)

        og_txb_ip = db_test.key_exist_in_object('og_txb_ip',dic_)
        og_txb_ver = db_test.key_exist_in_object('og_txb_ver',dic_)
        og_txb_area = db_test.key_exist_in_object('og_txb_area',dic_)

        if record_mode == "0":# update file
            db_test.write_temp(dic_,0,"add")# add only call once, Overwrite status & relation
        elif record_mode == "1":
            sqlcmd="insert into dfh.tb_vip_relation (ip,version,area,max_link,pro,act)values(\'"+txb_ip+"\',\'\',\'\',\'100\',\'S\',\'F\');"
            db_test.DB_update_connect(sqlcmd)
        db_test.write_config(dic_)
        DB_log('DEBUG','add_vip',"==== add_vip complete ====")
def del_vip(dic_,write_file_TF):
    if write_file_TF:
        txb_sn = db_test.key_exist_in_object('txb_sn',dic_)
        txb_ip = db_test.key_exist_in_object('txb_ip',dic_)
        txb_ver = db_test.key_exist_in_object('txb_ver',dic_)
        txb_area = db_test.key_exist_in_object('txb_area',dic_)
        curr_link = db_test.key_exist_in_object('curr_link',dic_)
        txb_max_link = db_test.key_exist_in_object('txb_max_link',dic_)
        txb_pro = db_test.key_exist_in_object('txb_pro',dic_)
        txb_act = db_test.key_exist_in_object('txb_act',dic_)

        og_txb_ip = db_test.key_exist_in_object('og_txb_ip',dic_)
        og_txb_ver = db_test.key_exist_in_object('og_txb_ver',dic_)
        og_txb_area = db_test.key_exist_in_object('og_txb_area',dic_)
        if record_mode == "0":# update file
            db_test.write_temp(dic_,0,"del")
        elif record_mode == "1":
            if len(og_txb_ip)>0:
                sqlcmd="delete from dfh.tb_vip_relation where ip = \'"+txb_ip+"\'"
                db_test.DB_update_connect(sqlcmd)
            if len(og_txb_ver)>0:
                sqlcmd="update dfh.tb_vip_relation set version = \'\',last_update = timezone(\'CCT\'::text, now()) where version = \'"+txb_ver+"\'"
                db_test.DB_update_connect(sqlcmd)
            if len(og_txb_area)>0:
                sqlcmd="update dfh.tb_vip_relation set area = \'\',last_update = timezone(\'CCT\'::text, now()) where area = \'"+txb_area+"\'"
                db_test.DB_update_connect(sqlcmd)
        db_test.write_config(dic_)
        DB_log('DEBUG','del_vip',"==== del_vip complete ====")
def query_sn_ip_from_relation():
    if record_mode == "0":
        results = db_test.read_information_from_temp(1)
    if record_mode == "1":
        sqlcmd = "select sn,ip from dfh.tb_vip_relation ORDER BY sn"
        results = db_test.DB_select_connect(sqlcmd)
    return results

def DB_log(log_level,function_name,debug_string):
    if record_mode == "0":
        #write_log.txt
        dic = {}
        dic.update({'log_level':log_level})
        dic.update({'function_name':function_name})
        dic.update({'debug_string':debug_string+", "+str(datetime.datetime.now())})
        db_test.write_log_txt(dic)
    if record_mode == "1":
        DB_str_ = db_test.DB_first_connect()
        try:
            DB_conn_ = psycopg2.connect(DB_str_)
        except:
            #print "Can't connect to Working DB"
            sys.exit(0)
        DB_conn_.autocommit = True
        DB_conn_.set_client_encoding('BIG5')
        DB_cursor_ = DB_conn_.cursor()
        DB_cursor_.execute("insert into dfh.tb_broker_log(log_level,msg,error_msg) VALUES (\'"+log_level+"\',\'"+function_name+"\',\'"+debug_string+"\')")
# socket test 20151201 start
#vip = ip.ip.ip.ip:port, 
#int_ = 73(current_link for securities), 75(version for securities), 
#       98(current_link and all information for diamond vip)
def count_vip_socket(vip,og_curr_link,int_):
    curr_link_result = 0
    viplist = vip.split(':')
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((viplist[0],4300))#60.250.174.141:4300
        sock.settimeout(10)
        #sock.connect(('192.168.12.112', 4300))
        if int_ == 73:
            sock.send(chr(0x73).encode('ascii'))
            data = sock.recv(32)
            curr_link = int.from_bytes(data[2:3], byteorder='little')
        elif int_ == 75:
            #version
            sock.send(chr(0x75).encode('ascii'))
            data = sock.recv(32)
            ver1 = int.from_bytes(data[2:3], byteorder='little')
            ver2 = int.from_bytes(data[3:4], byteorder='little')
            ver3 = int.from_bytes(data[4:5], byteorder='little')
            ver4 = int.from_bytes(data[5:6], byteorder='little')
        elif int_ == 98:
            #sock.connect(('192.168.12.122', 4300))
            sock.send(chr(98).encode('ascii'))
            data = sock.recv(32)
            curr_link = int.from_bytes(data[8:9], byteorder='little')
            #print(curr_link)
        DB_log('DEBUG','count_vip_socket sock.recv',str(data))
        sock.close()
    except:
        curr_link = 0
        DB_log('ERROR','count_vip_socket',viplist[0]+" socket fail")

    #if int(og_curr_link) >= curr_link:
        #curr_link_result = int(og_curr_link)
    #else:#curr_link > og_curr_link
    curr_link_result = curr_link
    if record_mode == "0":# update file
        dic = {}
        dic.update({'txb_ip':vip})
        dic.update({'curr_link':curr_link_result})
        if count_vip_socket_lock.acquire(1):
            db_test.write_temp(dic,0,'update_relation')# 0 = status
            count_vip_socket_lock.release()
    elif record_mode == "1":# update DB
        sqlcmd = "update dfh.tb_vip_status set curr_link = \'"+str(curr_link_result)+"\',last_update = timezone(\'CCT\'::text, now()) where ip = \'"+str(vip)+"\'"
        DB_update_connect(sqlcmd)
    DB_log("DEBUG","tick_to_update_curr_link","ip:"+vip+",og_curr_link:"+str(og_curr_link)+",curr_link:"+str(curr_link))

def vip_distribution(ver_,area_):
    group_vip_list = []
    lowest = 999
    vip_lowest = ""
    if record_mode == "0":
        tb_vip_status_list = db_test.read_information_from_temp(0)
        tb_vip_relation_list = db_test.read_information_from_temp(1)
    elif record_mode == "1":
        tb_vip_status_list = DB_select_connect("select * from dfh.tb_vip_status")
        tb_vip_relation_list = DB_select_connect("select * from dfh.tb_vip_relation")
    for r in tb_vip_relation_list:
        if r[2] == ver_ and r[3] == area_ and r[6] == 'T':
            group_vip_list.append(r[1])
    #find lowest
    dic_ = {}
    for r in group_vip_list:
        for rr in tb_vip_status_list:
            if r == rr[0]:
                if int(rr[1])<int(lowest):
                    lowest = int(rr[1])
                    vip_lowest = rr[0]
    #curr_link = lowest+1#new login vip distribution, so count +1
    curr_link = lowest#20171108 count +0
    dic_.update({'txb_ip':vip_lowest})
    dic_.update({'curr_link':str(curr_link)})
    if vip_distribution_lock.acquire(1):
        db_test.write_temp(dic_,0,'update_relation')
        vip_distribution_lock.release()
    #load>80%, 20151224
    #    curr_link/nax_link
    return vip_lowest

def create_cert():
    global login_seq
    now_time = str(datetime.datetime.now().time())
    vip_login_no = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[int(now_time[0:2]):int(now_time[0:2])+1] + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"[int(now_time[3:5]):int(now_time[3:5])+1] + str(login_seq+1).zfill(4)
    if login_seq == 9999:
        login_seq = 0
    login_seq = login_seq +1
    return vip_login_no
def tick_to_update_curr_link(int_):# int_ = update seq second
    global login_seq
    ip = ""
    while True:
        time.sleep(int_)
        tick_to_check_cert_online()
        if record_mode == "0":
            status_list = db_test.read_information_from_temp(0)
        elif record_mode == "1":
            status_list = db_test.DB_select_connect("select * from dfh.tb_vip_status")
        for r in status_list:
            curr_link = ""
            ip = r[0]
            if r[3] == "S" and r[4] == 'T':
                try:
                    count_vip_socket(ip,r[1],73)#update_curr_link
                except:
                    DB_log("ERROR","tick_to_update_curr_link","ip:"+ip+",count_vip_socket 73 fail")
            elif r[3] == "D" and r[4] == 'T':
                try:
                    count_vip_socket(ip,r[1],98)#update_curr_link
                except:
                    DB_log("ERROR","tick_to_update_curr_link","ip:"+ip+",count_vip_socket 98 fail")
def tick_to_check_cert_online():
    print('tick_to_check_cert_online now active'+str(datetime.datetime.now()))
    cert_collect = []
    dic = {}
    if record_mode == "0":
        tb_vip_certification_list = db_test.read_information_from_temp(2)
    elif record_mode == "1":
        tb_vip_certification_list = db_test.DB_select_connect("select * from dfh.tb_vip_certification")
    for r in tb_vip_certification_list:
        if int(r[4]) >= 2:
            cert_collect.append(r[3])
        elif int(r[4]) < 2:
            if r[1] == 'slogin':
                logout(r[3])
            else:
                if(len(find_cert(r[1],r[2])) > 0):
                    #logout(find_cert(r[1],r[2]))
                    logout(r[3])
                    DB_log("DEBUG","tick_to_check_cert_online force logout","id:"+r[1]+",pwd:"+r[2]+",cert:"+r[3])
    if len(cert_collect) > 0:
        for item in cert_collect:
            dic.update({'login_cert':item})#update cert to 1
            db_test.write_temp(dic,2,'set')
count_vip_socket_lock = threading.Lock()
vip_distribution_lock = threading.Lock()
if __name__ == '__main__':
    tick_to_update_curr_link(900)
