import pymongo as mg
from datetime import datetime

import deepquant.common.state_machine as sm
import deepquant.common.datetime_util as datetime_util
from deepquant.market_set.trade_dto import TfexTradeAction


class DataSetDAO :

    def __init__(self, db_con=None):
        """
        Set database connection
        :param db_conn: an already connected database connection
        """
        try:
            if db_con is not None:
                self.__db = db_con.db
        except:
            raise

    def connect(self, db_host, db_port, db_name):
        """
        Connect to database using specified db_name
        :param db_host: database's host name in string
        :param db_port: database's port number in integer
        :param db_name: database's name in string
        """
        try:
            self.__db_conn = mg.MongoClient(db_host, db_port)
            self.__db = self.__db_conn[db_name]
        except:
            raise

    # Close database connection
    def close_connection(self):
        self.__db_conn.close()

    # Load ข้อมูลทุก row (หรือเรียกว่า documents) จาก db_collection_name ใน mongodb
    # Param: db_collection_name - database collection's name in string
    # Param: dataset_session_maxrow - จำนวน row (หรือจำนวน document) สูงสุดที่จะดึงจาก db_collection_name
    # Param: order - การเรียงลำดับ: 1=ascending, -1=descending
    def load(self, db_collection_name, dataset_session_maxrow, order):
        last_documents = self.__db[db_collection_name].find().sort(
            [('date', order)]).limit(dataset_session_maxrow)
        return last_documents

    # เพิ่ม row ใหม่เข้าไปในฐานข้อมูล
    # Param: db_collection_name - database collection's name in string
    # Param: new_doc - new document (หรือ row ใหม่) ที่จะ insert ลง db_collection_name โดย new_doc เป็น dataframe
    def insert_row(self, db_collection_name, new_doc):
        new_data = new_doc.copy()
        new_data = new_data.reset_index()
        new_data.rename(columns={'index':'date'}, inplace=True)

        new_data_dict = new_data.to_dict('records')
        for i in range(0, len(new_data_dict)):
            record = new_data_dict[i]
            record['_id'] = new_data.loc[i, 'date'].timestamp() * 1000

        # Insert into DB collection
        self.__db[db_collection_name].insert_one(new_data_dict.pop())

    # เพิ่ม trade action ใหม่เข้าไปในฐานข้อมูล
    # Param: robot_name - name of trading robot
    # Param: db_collection_name - database collection's name of trade action in string
    # Param: trade_action - new trade action (หรือ row ใหม่) ที่จะ insert ลง db_collection_name
    # โดย trade_action เป็นอ็อบเจ็คต์ของคลาส TfexTradeAction
    # Param: created_datetime - เป็นชนิด datetime
    # Param: equity - current equity in portfolio
    # Param: account_id - TFEX account_id
    def insert_tfex_trade_action(self, robot_name, db_collection_name, trade_action, created_datetime, equity, account_id):
        # example: '2018-03-31 15:30:00'
        trade_action_dt = datetime.strptime(trade_action.datetime, '%Y-%m-%d %H:%M:%S')

        scale_size = 0.0
        if trade_action.action_code == sm.StateMachine.ACTION_SCALE_IN_BUY \
                or trade_action.action_code == sm.StateMachine.ACTION_SCALE_IN_SELL:
            scale_size = trade_action.volume
            trade_action.volume = 0

        elif trade_action.action_code == sm.StateMachine.ACTION_SCALE_OUT_BUY \
                or trade_action.action_code == sm.StateMachine.ACTION_SCALE_OUT_SELL:
            scale_size = -1 * trade_action.volume
            trade_action.volume = 0

        new_trade_action = {
            'created_datetime'  : created_datetime,
            'sys_acc_id'        : account_id,
            'date'              : trade_action_dt,
            'signal_code'       : trade_action.signal_code,
            'action_code'       : trade_action.action_code,
            'symbol'            : trade_action.symbol,
            'robot_name'        : robot_name,
            'possize'           : trade_action.volume,
            'manipulate_possize': float(scale_size),
            'slippage'          : float(trade_action.slippage),
            'action_price'      : float(trade_action.action_price),
            'stop_loss_pip'     : float(trade_action.stop_loss),
            'stop_loss_price'   : 0.0,
            'equity'            : float(equity)
        }

        # Insert into DB collection
        self.__db[db_collection_name].insert_one(new_trade_action)

    def load_tfex_trade_action(self, db_collection_name, account_id):
        # Load latest trade action
        doc = self.__db[db_collection_name].find({"sys_acc_id": account_id}).sort({"date": -1}).limit(1)

        trade_action = TfexTradeAction()
        trade_action.symbol = doc['symbol']
        trade_action.datetime = doc['date']
        trade_action.action_code = doc['action_code']
        trade_action.signal_code = doc['signal_code']
        trade_action.stop_loss = doc['stop_loss_pip']
        trade_action.volume = doc['possize']
        trade_action.action_price = doc['action_price']

        if trade_action.action_code == sm.StateMachine.ACTION_SCALE_IN_BUY \
            or trade_action.action_code == sm.StateMachine.ACTION_SCALE_IN_SELL \
            or trade_action.action_code == sm.StateMachine.ACTION_SCALE_OUT_BUY \
            or trade_action.action_code == sm.StateMachine.ACTION_SCALE_OUT_SELL :

            # ในระบบ SET execution เก็บ position size ที่จะ scale in/out ไม่ใช่เปอร์เซ็นต์
            # เช่น 40 หมายถึง scale in 40%, -20 หมายถึง scale out 20%
            # แต่ใน python robot ใช้เป็นเปอร์เซ็นต์
            # เช่น 0.4 หมายถึง scale in 40%
            # จึงต้องแปลงค่าและเครื่องหมาย (+/-) ก่อน
            trade_action.volume = abs(round(doc['manipulate_possize'] / 10, 2))

        return trade_action
