import pandas as pd
from deepquant.common.base_class import BaseCommission

class CommissionTable_TFEX_S50(BaseCommission):
    __data_list = [[1, 25, 85], [26, 100, 65], [101, 500, 46], [501, 100000000, 36]]
    __comm = pd.DataFrame(data=__data_list, columns={'from', 'to', 'rate'})


    # Returns commission rate corresponding to specified 'pos_size'
    # pos_size - position size, data type is float
    def get_comm_rate(self, pos_size):
        comm_rate = 0.0

        for i in range(len(self.__comm)):
            from_size = self.__comm.loc[i, 'from']
            to_size = self.__comm.loc[i, 'to']
            rate = self.__comm.loc[i, 'rate']

            if from_size <= pos_size and to_size >= pos_size:
                comm_rate = rate
                break

        return comm_rate