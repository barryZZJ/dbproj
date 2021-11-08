import os
import pandas as pd
from typing import List, Dict, Tuple, Union

class Meta(dict):
    def __init__(self, name, datatype: str, default_val, primary_key=False, not_null=False):
        data = {
            'name': name,
            'datatype': datatype,
            'primary_key': primary_key,
            'not_null': not_null,
            'default_val': default_val
        }
        super(Meta, self).__init__(data)

class MetaTable:
    def __init__(self, name, metas: List[Meta]=None):
        self.name = name
        self.metas = pd.DataFrame(metas or [])

    def __repr__(self):
        return self.metas.__repr__()

    def getMetas(self):
        return self.metas

    def appendMeta(self, meta: Meta):
        self.metas = self.metas.append(meta, ignore_index=True)

    def removeMeta(self, name):
        self.metas.drop(index=self.metas.loc[self.metas['name']==name].index, inplace=True)

    def updateMeta(self, name, newMeta: Meta):
        self.metas.loc[self.metas['name'] == name] = [newMeta[k] for k in self.metas]

    def findMeta(self, name):
        return self.metas.loc[self.metas['name'] == name]

    def save(self, filepath='metaTable'):
        self.metas.to_excel(os.path.join(filepath, f'{self.name}.xls'), sheet_name=self.name, index=False)

    @staticmethod
    def load(filename:str):
        assert filename.endswith('.xls')
        name = filename[:-4]
        df = pd.read_excel(filename)
        metaTable = MetaTable(name)
        metaTable.metas = df
        return metaTable

class _DataRow(dict):
    def __init__(self, metaTable:MetaTable, data:Union[Dict, List, Tuple]):
        if isinstance(data, List) or isinstance(data, Tuple):
            assert len(data) == len(metaTable.getMetas())
            super(_DataRow, self).__init__(zip(metaTable.getMetas()['name'], data))

        elif isinstance(data, Dict) and len(data) != len(metaTable.getMetas()):
            # 自动填充没有赋值的字段
            for _, meta in metaTable.getMetas().iterrows():
                if meta['primary_key']: assert data.get(meta['name'])
                data.setdefault(meta['name'], meta['default_val'] if meta['not_null'] else None)
            super(_DataRow, self).__init__(data)
        else:
            super(_DataRow, self).__init__(data)


class DataTable:
    def __init__(self, metaTable:MetaTable, datas: List[Union[Dict, List, Tuple]]=None):
        self.metaTable = metaTable
        datas = [_DataRow(self.metaTable, data) for data in datas] if datas else []
        self.table = pd.DataFrame(data=datas, columns=[meta['name'] for _,meta in self.metaTable.getMetas().iterrows()])

    def __repr__(self):
        return self.table.__repr__()

    def append(self, data:Union[Dict, List, Tuple]):
        data = _DataRow(self.metaTable, data)
        self.table = self.table.append(data, ignore_index=True) # type:pd.DataFrame

    def remove(self, searchKey, searchVal):
        self.table.drop(index=self.table.loc[self.table[searchKey]==searchVal].index, inplace=True)

    def update(self, searchKey, searchVal, newdata:Union[Dict, List, Tuple]):
        newdata = _DataRow(self.metaTable, newdata)
        self.table.loc[self.table[searchKey] == searchVal] = [newdata[k] for k in self.table]

    def find(self, searchKey, searchVal):
        return self.table.loc[self.table[searchKey] == searchVal]

    def save(self, filepath='dataTable'):
        self.table.to_excel(os.path.join(filepath, f'{self.metaTable.name}.xls'), sheet_name=self.metaTable.name, index=False)

    @staticmethod
    def load(filename, metaTable):
        assert filename.endswith('.xls')
        df = pd.read_excel(filename)
        dataTable = DataTable(metaTable)
        dataTable.table = df
        return dataTable

if __name__ == '__main__':
    metas = [
        Meta('id', 'str', '', True, True),
        Meta('name', 'str', '', False, False),
        Meta('dept', 'str', '', False, False),
        Meta('age', 'int', 0, False, False),
        Meta('gender', 'str', '', False, False),
    ]
    name = 'student'
    metaTable = MetaTable(name, metas)
    metaTable.save('metaTable')

    datas = [
        ('S1', 'Wangfeng',  'Physics',  20, 'M'),
        ('S2', 'Liu fang',  'Physics',  19, 'M'),
        ('S3', 'Chen yun',  'CS',       22, 'M'),
        ('S4', 'Wu kai',    'Finance',  19, 'M'),
        ('S5', 'Liu li',    'CS',       21, 'F'),
        ('S6', 'Dongqing',  'Finance',  18, 'F'),
        ('S7', 'Li',        'CS',       19, 'F'),
        ('S8', 'Chen',      'CS',       21, 'F'),
        ('S9', 'Zhang',     'Physics',  19, 'M'),
        ('S10','Yang',      'CS',       22, 'F'),
    ]

    dataTable = DataTable(metaTable, datas)
    dataTable.save('dataTable')

    metaTable = MetaTable.load('metaTable/student.xls')
    metaTable.metas.fillna('', inplace=True)
    dataTable = DataTable.load('dataTable/student.xls', metaTable)
    print(metaTable)
    print(dataTable)

    print('元数据表：')
    print('添加前：\n', metaTable)
    newMeta = Meta('test', 'int', 0, False, False)
    print('\n查找gender字段：\n', metaTable.findMeta('gender'))
    metaTable.appendMeta(newMeta)
    print('\n添加test字段后：\n', metaTable)
    metaTable.removeMeta('age')
    print('\n删除age字段后：\n', metaTable)
    newMeta = Meta('test2', 'str', '', True, True)
    metaTable.updateMeta('test', newMeta)
    print('\n更新test字段后：\n', metaTable)

    print('数据表：')
    print('添加前：\n', dataTable)
    print('\n查找id为S2的数据：\n', dataTable.find('id', 'S2'))
    print('\n查找age为19的数据：\n', dataTable.find('age', 19))
    newData = ('S11', 'Wang', 'CS', 19, 'F')
    dataTable.append(newData)
    print('\n添加S11后：\n', dataTable)
    dataTable.remove('dept', 'CS')
    print('\n删除dept为CS的数据后：\n', dataTable)
    newData = ('S2', 'Ma fei', 'CS', 23, 'F')
    dataTable.update('id', 'S2', newData)
    print('\n更新id为S2的字段后：\n', dataTable)
