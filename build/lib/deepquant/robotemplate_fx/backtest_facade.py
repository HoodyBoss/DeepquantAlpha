import deepquant.robotemplate_fx.model.model_helper as model_helper

@staticmethod
def get_robot_config(**kwargs):
    return model_helper.get_robot_config(**kwargs)

@staticmethod
def cal_risk_to_take(**kwargs):
    return model_helper.cal_risk_to_take(**kwargs)

@staticmethod
def cal_entry_pos_size(**kwargs):
    return model_helper.cal_entry_pos_size(**kwargs)

@staticmethod
def generate_trade_label(**kwargs):
    return model_helper.generate_trade_label(**kwargs)
