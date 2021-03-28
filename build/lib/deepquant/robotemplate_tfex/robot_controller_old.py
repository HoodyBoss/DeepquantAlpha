# from flask import current_app
# import pandas as pd
import asyncio
import logging
import sys, traceback

from deepquant.common.dataset_dao import DataSetDAO
from deepquant.market_set.trade_dto import TradeOutput

import deepquant.common.error as err
import deepquant.common.state_machine as st_machine
import deepquant.common.datetime_util as datetime_util
import deepquant.market_set.trade_dto as trade_dto

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class BaseRobotController:
    _state_machine = st_machine.StateMachine()

    # Initialize robot controller
    def __init__(self, robot_context, alpha_model, portfolio_model, risk_model, transcost_model, execution_model):
        # ==============================================================================================================
        # NOTE:
        # __xxx = private class variable
        # _xxx = protected class variable
        # xxx = public class variable
        # ==============================================================================================================

        # Create robot context จัดเก็บเป็น in-memory data โดยจัดเก็บอยู่ในหน่วยความจำ
        # robot context เปรียบเสมือนที่เก็บข้อมูลชั่วคราวระหว่างที่โรบอททำงาน
        self._context = robot_context

        # Create core models for robot engine
        self._alpha_model = alpha_model
        self._portfolio_model = portfolio_model
        self._risk_model = risk_model
        self._transcost_model = transcost_model
        self._execution_model = execution_model

        # Set robot context ให้กับแต่ละโมเดล โดยทุกโมเดลจะใช้ robot context ตัวเดียวกัน เพื่อประหยัดหน่วยความจำ
        # และประมวลผลเร็วขึ้น
        self._alpha_model.set_robot_context(self._context)
        self._portfolio_model.set_robot_context(self._context)
        self._risk_model.set_robot_context(self._context)
        self._transcost_model.set_robot_context(self._context)
        self._execution_model.set_robot_context(self._context)

    def get_robot_context(self):
        return self._context

    # Initialize DAO (Data Access Object), a database gateway object
    def get_dao(self):
        db_host = self._context.config['db_host']
        db_port = self._context.config['db_port']
        db_name = self._context.config['db_name']
        dao = DataSetDAO()
        dao.connect(db_host, db_port, db_name)
        return dao

    async def run_execution_model(self, future, trade_output, trade_input, trade_action, account_id, dao):
        """
        เมธอดนี้ทำหน้าที่ call เมธอด execute ของ execution model เพื่อทำการ execute trade โดยการไป call ระบบ execution อีกที
        หลังจากนั้นจะบันทึก trade action ลง database
        :param future: ตัวแปรที่ใช้ประกอบการประมวลผล asynchronous ใช้รับผลลัพธ์หลังประมวลผลใน task เสร็จ
        :param trade_output: an instance of TradeOutput
        :param trade_input: an instance of TradeInput
        :param trade_action: an instance of TfexTradeAction
        :param account_id: TFEX account id (username)
        :param dao: an instance of DataSetDAO
        :return: the result is the trade_output that has been wrapped within the future object
        """
        try:
            response_msg = self._execution_model.execute(trade_input, trade_action, account_id)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "[{}] Response message after called execution model: {}".format(account_id, response_msg))
            await asyncio.sleep(2)

            # Insert latest trade action into database
            db_collection_name = self._context.config['db_collection_name'] + '_action'
            created_datetime = datetime_util.utcnow()
            # Calculate equity value to be used for calculating position size
            equity = trade_input.account_info['equity'] + self._context.config['equity_outside_broker']
            dao.insert_tfex_trade_action(self._context.config['robot_name'], db_collection_name, trade_action \
                                         , created_datetime, equity, account_id)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "[{}] Insert new trade action into database successful".format(account_id))

            trade_output.response_code = trade_dto.TradeOutput.http_success_code

        except Exception as e:
            trade_output.response_code = trade_dto.TradeOutput.http_error_code
            response_msg = str(e)
        finally:
            try:
                if dao is not None:
                    dao.close_connection()
            except Exception as e:
                trade_output.response_code = trade_dto.TradeOutput.http_error_code
                response_msg = "[{}] Close connection from database error: {}".format(account_id, e)

        trade_output.response_message = response_msg
        future.set_result(trade_output)

    async def run_trade_task(self, future, trade_input, trade_action, account_id):
        """
        เมธอดนี้เป็นเมธอดหลักของ robot engine ในการ call โมเดลต่างๆ ได้แก่ alpha, risk, transaction cost,
        portfolio construction, execution
        :param future: ตัวแปรที่ใช้ประกอบการประมวลผล asynchronous ใช้รับผลลัพธ์หลังประมวลผลใน task เสร็จ
        :param trade_input: an instance of TradeInput
        :param trade_action: an instance of TfexTradeAction
        :param account_id: TFEX account id (username)
        :return:
        """

        response_code = trade_dto.TradeOutput.http_error_code

        try:
            try:
                dao = self.get_dao()
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self._context.config['robot_name'], 'INFO' \
                            , "[{}] RobotController: initialize DataSetDAO successful".format(account_id))
            except Exception as e:
                logger.error('%s - %s - [%s][%s]: %s' \
                             , datetime_util.bangkok_now(), __name__ \
                             , self._context.config['robot_name'], 'ERROR' \
                             , "[{}] RobotController: initialize DataSetDAO failed!".format(account_id))
                raise err.TradeRobotError("Initialize DataSetDAO failed in method run_trade_task -> {}".format(e))

            # =============================================================================================================
            # 1) Determine mode ('trade' or 'backtest') for connect to market
            # If mode = 'trade' -> login to market, load orders, load porfolio info, account info
            # If mode = 'backtest' -> prepare initial data: money
            # =============================================================================================================
            if self._context.config['mode'] == 'trade':
                # Login to market, load order list, load porfolio info, account info
                self._execution_model.prepare_execution(trade_input, account_id)
                self._portfolio_model.set_cur_trade_position(trade_input)
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self._context.config['robot_name'], 'INFO' \
                            , "[{}] Prepare execution and set current trade position sucessful".format(account_id))
                # elif self.__config['mode'] == 'backtest':
                # NOTE: ยังไม่ได้ใส่โค้ดตัวอย่าง

            # =============================================================================================================
            # 2) Select trade action code
            # =============================================================================================================
            cur_trade_position = trade_input.cur_trade_position
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "[{}] Current trade position is {}".format(account_id, cur_trade_position))
            # **************************************************************************************************************
            trade_action.action_code = self._state_machine.get_trade_action(trade_action.signal_code, cur_trade_position)
            # **************************************************************************************************************
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "[{}] Selected trade action is {}".format(account_id, trade_action.action_code))

            # =============================================================================================================
            # 3) Run risk model, transaction cost model, portfolio construction model
            # =============================================================================================================
            # **************************************************************************************************************
            trade_action = self._risk_model.run(trade_input, trade_action)
            trade_action = self._transcost_model.run(trade_input, trade_action)
            trade_action = self._portfolio_model.run(trade_input, trade_action)
            # **************************************************************************************************************
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "[{}] Run models: risk, transaction cost, portfolio construction successful".format(account_id))

            response_code = trade_dto.TradeOutput.http_success_code

        except err.DeepQuantError as dq_error:
            response_msg = "[{}] DeepQuantError in method run_trade_task: {}".format(account_id, dq_error)
        except err.TradeModelError as tm_error:
            response_msg = "[{}] TradeModelError in method run_trade_task: {}".format(account_id, tm_error)
        except Exception as e:
            response_msg = "[{}] Unexpected error in method run_trade_task: {}".format(account_id, e)

        # Create trade output
        trade_output = trade_dto.TradeOutput()
        trade_output.response_code = response_code
        trade_output.trade_account_id = account_id

        # =============================================================================================================
        # 4) Determine mode ('trade' or 'backtest') for execution
        # If mode = 'trade' -> call execution model to execute trade
        # =============================================================================================================

        if trade_output.response_code == trade_dto.TradeOutput.http_success_code:
            if self._context.config['mode'] == 'trade':
                # Execute trade
                await self.run_execution_model(future, trade_output, trade_input, trade_action, account_id, dao)
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self._context.config['robot_name'], 'INFO' \
                            , "[{}] Run execution model successful".format(account_id))

                # elif self.__config['mode'] == 'backtest':
                # NOTE: ยังไม่ได้ใส่โค้ดจัดการกรณี backtest
        elif trade_output.response_code == trade_dto.TradeOutput.http_error_code:
            trade_output.response_message = response_msg
            future.set_result(trade_output)

    def run_trade_async(self, trade_input):
        final_trade_output = TradeOutput()

        try:
            try:
                dao = self.get_dao()
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self._context.config['robot_name'], 'INFO' \
                            , "RobotController: initialize DataSetDAO successful")
            except Exception as e:
                logger.error('%s - %s - [%s][%s]: %s' \
                             , datetime_util.bangkok_now(), __name__ \
                             , self._context.config['robot_name'], 'ERROR' \
                             , "RobotController: initialize DataSetDAO failed!")
                raise err.TradeRobotError("[{}] Initialize DataSetDAO failed in method run_trade_async -> {}".format(
                    self._context.config['robot_name'],
                    e))

            price_df = trade_input.price_df
            indi_df = trade_input.indi_df
            feature_df = trade_input.feature_df

            # =============================================================================================================
            # 1) Prepare raw data (price & indicators) into robot context
            # =============================================================================================================
            # NOTE: ข้อมูลเหล่านี้จะถูกเรียกใช้ได้ตรงๆ เพื่อสนับสนุนการเขียนโค้ด rule based
            # self._context.prepare_rulebased_data()
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "Prepare variables for rules base model successful")

            # =============================================================================================================
            # 2) Call alpha model
            # =============================================================================================================
            # trade_action is an instance of TfexTradeAction
            # **************************************************************************************************************
            trade_action = self._alpha_model.run(trade_input)
            # **************************************************************************************************************
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "Run alpha model successful")

            # =============================================================================================================
            # 3) Handle asynchronous tasks
            # =============================================================================================================
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()

            account_ids = self._context.config['account_ids']
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "Binding trading accounts: " + str(account_ids))

            futures = []
            for i in range(0, len(account_ids)):
                futures.append(asyncio.Future())

            tasks = []
            for i in range(0, len(account_ids)):
                # Clone trade_input to new instance because each trade_input binds to each trade account
                clone_trade_input = trade_input.clone()
                # Clone trade_action to new instance because each trade_action binds to each trade account
                clone_trade_action = trade_action.clone()

                tasks.append(self.run_trade_task( \
                    futures[i], \
                    clone_trade_input, \
                    clone_trade_action, \
                    account_ids[i]))

            loop.run_until_complete(asyncio.wait(tasks))
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "Run asynchronous task successful")

            results = []
            for i in range(0, len(account_ids)):
                results.append(futures[i].result())

            loop.close()

            # =============================================================================================================
            # 4) Insert latest data (price, indicators, features, trade action analysis logs) into database
            # =============================================================================================================
            data = self._context.dataset_context.get_dataset_last_row()
            dao.insert_row(self._context.config['db_collection_name'], data)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self._context.config['robot_name'], 'INFO' \
                        , "Insert latest price/indicators/features into database successful, DB collection: "
                        + self._context.config['db_collection_name'])

            # =============================================================================================================
            # 5) Handle execution results
            # =============================================================================================================
            error_trade_outputs = []
            # Collect failed trade outputs
            for i in range(0, len(results)):
                trade_output = results[i]
                if trade_output.response_code == TradeOutput.http_error_code:
                    error_trade_outputs.append(trade_output)

            total_trade_outputs = len(error_trade_outputs)

            if total_trade_outputs == 0:
                final_trade_output.response_code = TradeOutput.http_success_code
                final_trade_output.response_message = 'Execution success'
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self._context.config['robot_name'], 'INFO' \
                            , 'Execution success')
            else:
                final_trade_output.response_code = TradeOutput.http_error_code
                error_acc_ids = ''

                for i in range(0, total_trade_outputs):
                    if i == 0:
                        error_acc_ids = str(error_trade_outputs[i].trade_account_id)
                    elif i > 0:
                        error_acc_ids = error_acc_ids + ', ' + str(error_trade_outputs[i].trade_account_id)

                    logger.info('%s - %s - [%s][%s]: %s' \
                                , datetime_util.bangkok_now(), __name__ \
                                , self._context.config['robot_name'], 'INFO' \
                                , '[{}] Run trade task error: {}'.format( \
                            error_trade_outputs[i].trade_account_id,
                            error_trade_outputs[i].response_message))

                final_trade_output.response_message = 'Execution error: {}'.format(error_acc_ids)

        except Exception as e:
            final_trade_output.response_code = TradeOutput.http_error_code
            final_trade_output.response_message = "[{}] Error in BaseRobotController.run_trade_async -> {}".format(
                self._context.config['robot_name'],
                e)
            logger.error('%s - %s - [%s][%s]: %s' \
                         , datetime_util.bangkok_now(), __name__ \
                         , self._context.config['robot_name'], 'ERROR' \
                         , final_trade_output.response_message)

            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      file=sys.stdout)
        finally:
            try:
                if dao is not None:
                    dao.close_connection()
            except Exception as e:
                final_trade_output.response_code = TradeOutput.http_error_code
                final_trade_output.response_message = "[{}] Close DB connection error in run_trade_async -> {}".format(
                    self._context.config['robot_name'],
                    e)
                logger.error('%s - %s - [%s][%s]: %s' \
                             , datetime_util.bangkok_now(), __name__ \
                             , self._context.config['robot_name'], 'ERROR' \
                             , final_trade_output.response_message)

        return final_trade_output

    # =========================================================================================================================
    def close_all_pos(self):
        trade_output = trade_dto.TradeOutput()
        trade_output.response_code = trade_dto.TradeOutput.http_error_code

        error_msg = 'Close positions\n'

        account_ids = self._context.config['account_ids']
        logger.info('%s - %s - [%s][%s]: %s' \
                    , datetime_util.bangkok_now(), __name__ \
                    , self._context.config['robot_name'], 'INFO' \
                    , "Loaded all trading accounts: " + str(account_ids))

        total_success_acc = 0

        for i in range(0, len(account_ids)):
            try:
                account_id = account_ids[i]

                # Create trade input
                symbol = self._context.config['symbol']
                close_signal_code = self._state_machine.SIGNAL_CLOSE_AND_WAIT
                signal_dict = {'signal_code': close_signal_code, 'stop_loss': 0.0, \
                               'entry_pos_size_percent': 0.0, 'scale_size': 0.0}
                trade_input = trade_dto.TradeInput(symbol, signal_dict=signal_dict)

                # Clone trade_input to new instance because each trade_input binds to each trade account
                clone_trade_input = trade_input.clone()

                # Prepare execution
                if self._context.config['mode'] == 'trade':
                    # Login to market, load order list, load porfolio info, account info
                    self._execution_model.prepare_execution(clone_trade_input, account_id)

                    # Create trade action
                    trade_action = trade_dto.TfexTradeAction()
                    cur_position = clone_trade_input.cur_trade_portfolio_entry['position'].lower()
                    if cur_position == 'long':
                        trade_action.action_code = self._state_machine.ACTION_CLOSE_BUY
                    elif cur_position == 'short':
                        trade_action.action_code = self._state_machine.ACTION_CLOSE_SELL

                    if cur_position == 'long' or cur_position == 'short':
                        response_msg = self._execution_model.execute(trade_input, trade_action, account_id)
                        logger.info('%s - %s - [%s][%s]: %s' \
                                    , datetime_util.bangkok_now(), __name__ \
                                    , self._context.config['robot_name'], 'INFO' \
                                    , "[{}] Response message after called execution model: {}".format(account_id, response_msg))

                        total_success_acc = total_success_acc + 1

            except Exception as e:
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self._context.config['robot_name'], 'INFO' \
                            , "Close position error: " + str(account_id))
                error = "Close position error: %s, %s, %s".format(self._context.config['robot_name'], str(account_id), str(e))
                error_msg = error_msg + error + '\n'

        if total_success_acc == len(account_ids):
            trade_output.response_code = trade_dto.TradeOutput.http_success_code
            acc_ids_str = ', '.join(str(x) for x in account_ids)
            trade_output.response_message = 'Close positions successful: %s'.format(acc_ids_str)
        else:
            trade_output.response_message = error_msg

        return trade_output
