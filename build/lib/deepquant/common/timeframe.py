class Timeframe():
    tf_code = { 'M1':11, 'M2':12, 'M3':13, 'M4':14, 'M5':15, 'M10':16, 'M15':17, 'M30':18\
        , 'H1':19, 'H2':20, 'H3':21, 'H4':22, 'D1':23, 'W1':24 }

    @staticmethod
    def get_code(tf_str):
        code = None
        if tf_str is not None and tf_str in Timeframe.tf_code:
            code = Timeframe.tf_code[tf_str]
        return code

