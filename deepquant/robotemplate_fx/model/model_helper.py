
def get_robot_config(**kwargs):
    return kwargs['robot_config']


def cal_risk_to_take(**kwargs):
    """
    Calculate risk โดยประเมินจากจำนวนเปอร์เซ็นต์การขาดทุนสูงสุดที่รับได้จากเงินทุน (equity) ที่มี
    """
    risk = 0.0
    try:
        # Sample code
        if kwargs['balance'] <= 10000.0:
            risk = kwargs['base_risk']
        else:
            risk = kwargs['base_risk'] * 0.75
    except Exception as e:
        raise Exception('Calculate risk error: {}'.format(e))
    return risk


def cal_entry_pos_size(**kwargs):
    entry_pos_size = 0.0

    try:
        sl_pips = kwargs['stop_loss']

        balance = 0.0
        try:
            balance = kwargs['balance']
        except Exception as e:
            raise Exception("Balance is invalid: {}".format(e))

        balance = round(balance * kwargs['fund_allocate_size'], 2)

        stop_loss_pips = sl_pips
        if isinstance(sl_pips, int):
            stop_amount = float(sl_pips)

        # คำนวณความเสี่ยงของเทรดนี้ ซึ่งหมายถึง จ.น. เปอร์เซ็นต์ที่ยอมขาดทุนได้ในเทรดนี้
        base_risk = kwargs['base_risk']
        risk = cal_risk_to_take(balance=balance, base_risk=base_risk)

        # สูตรคำนวณ position size
        cal_pos_size_formula = kwargs['cal_pos_size_formula']

        # คำนวณ position size
        if cal_pos_size_formula == 1:
            limit_pos_size = kwargs['limit_pos_size']
            entry_pos_size = limit_pos_size

        elif cal_pos_size_formula == 2:
            cal_pos_size_formula2_size = kwargs['cal_pos_size_formula2_size']
            pos_size = (cal_pos_size_formula2_size * balance) / 1000
            # Adjust position size
            if kwargs['qty_percent'] is not None and kwargs['qty_percent'] > 0:
                pos_size = pos_size * kwargs['qty_percent']
            entry_pos_size = round(pos_size, kwargs['pos_size_decimal_num'])

        elif cal_pos_size_formula == 3:
            pos_size = (balance * risk) / ((stop_loss_pips / 10) * kwargs['point_value'])
            # Adjust position size
            if kwargs['qty_percent'] is not None and kwargs['qty_percent'] > 0:
                pos_size = pos_size * kwargs['qty_percent']
            entry_pos_size = round(pos_size, kwargs['pos_size_decimal_num'])

        elif cal_pos_size_formula == 4:
            if kwargs['qty_percent'] is not None and kwargs['qty_percent'] > 0:
                pos_size = (kwargs['qty_percent'] * balance) / 1000
            entry_pos_size = round(pos_size, kwargs['pos_size_decimal_num'])

    except Exception as e:
        raise Exception('{}'.format(e))

    return entry_pos_size


def generate_trade_label(**kwargs):
    try:
        if 'strategy_id' in kwargs:
            label = '{}{}{}{}'.format(kwargs['strategy_id'], kwargs['robot_id']\
                                        , kwargs['symbol_id'], kwargs['trade_model_id'])
        elif 'mock_label' in kwargs:
            label = kwargs['mock_label']
    except Exception as e:
        raise Exception('{}'.format(e))

    return label
