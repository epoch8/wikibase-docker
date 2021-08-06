import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, select, Date, JSON
import logging
import os
import sys
from math import ceil
from tqdm import tqdm
from pandas.testing import assert_frame_equal
from datetime import datetime

from wikibaseintegrator import wbi_core, wbi_login, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config




WIKIBASE_HOST = '84.201.142.182'

WIKIBASE_LOGIN = 'WikibaseAdmin'
WIKIBASE_PASSWORD = 'WikibaseDockerAdminPass'

wbi_config['MEDIAWIKI_API_URL'] = f'http://{WIKIBASE_HOST}:8181/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = f'http://{WIKIBASE_HOST}:8989/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'http://wikibase.svc'



def get_items_by_label(label_list:list, item_type:str, is_unique:bool = True, is_notnull:bool = True):
    '''
    По переданному списку лейблов находит entity_id в базе Wikibase. 
    
    label_list: список искомых лейблов
    item_type: тип искомого объекта. Если не указано, то любой объект. Если указано:
        "P" - Property
        "Q" - Item
    is_unique: если True, то вернёт ошибку, если найдено больше одного значения
    is_notnull: если True, то вернёт ошибку, если не найдено ни одного значения
    '''
    
    query = """
        SELECT DISTINCT ?item ?itemLabel
        WHERE {{
          ?item rdfs:label ?itemLabel. 

          VALUES ?itemLabel {{ {label_filter} }}
        }}""".format(label_filter = ' '.join([f'\"{i}\"@en' for i in label_list]))
    
    result = wbi_core.ItemEngine.execute_sparql_query(query)
    result_list = [[i['itemLabel']['value'], i['item']['value'].replace('http://wikibase.svc/entity/', '')] 
                   for i in result['results']['bindings']]
    
    df = pd.DataFrame(result_list, columns = ['label', 'item'])
        
    if item_type in ('P', 'Q'):
        df = df[df.item.str.contains(item_type)] 

    df_check = df.groupby('label').count()
    if is_unique and df_check.item.max() > 1:
        r = df[df.label.isin(df_check[df_check.item > 1].index.to_list())].sort_values(by = 'label')
        logging.info(f"entity_id определён неоднозначно: \n{r}")
        return None
    elif is_notnull and len(set(label_list) - set(df.label)) > 0:
        r = set(label_list) - set(df.label)
        logging.info(f"entity_id не найден: \n{r}!")
        return None
    else:
        return df
    
    
def get_wb_parent(Q:str, P:str, login_instance:wbi_login.Login) -> str:
    '''
    Свойством P какого объекта-родителя является объект Q? Возвращает ошибку, если родителей ноль или несколько.
        Q - целевой объект
        P - каким параметром он должен быть
    '''

    query = f'''
        SELECT ?entity_id ?entity_name WHERE {{
            ?entity_id wdt:{P} wd:{Q} .
            ?entity_id rdfs:label ?entity_name .
        }}'''
    print(query)
    result = wbi_core.ItemEngine.execute_sparql_query(query)

    result_list = [[i['entity_name']['value'], i['entity_id']['value'].replace('http://wikibase.svc/entity/', '')]
                   for i in result['results']['bindings']]

    result_df = pd.DataFrame(result_list, columns = ['entity_name', 'entity_id'])

    if result_df.shape[0] > 1:
        raise Exception(f'Object with entity_id {Q} have several parents: \n{result_df.entity_id.to_list()}')
    elif result_df.shape[0] == 0:
        raise Exception(f'Object with entity_id {Q} not finded in !')
    else:
        return (result_df.at[0, 'entity_name'], result_df.at[0, 'entity_id'])
    
    
def get_wb_statements(login_instance:wbi_login.Login, Q:str, P:str, Pq:str = 'P0') -> pd.DataFrame:
    '''
    Для объекта Q для заданного стейтмента вывести все его айтемы с заданными квалифаерами
    '''
    
    query = f'''
        SELECT ?STATEMENT_VALUE ?ITEM_LABEL ?QUALIFIER
        WHERE
        {{
             wd:{Q} p:{P} ?statement.
             ?statement ps:{P} ?STATEMENT_VALUE.

             OPTIONAL {{ ?statement pq:{Pq} ?QUALIFIER. }}     

             OPTIONAL {{ ?STATEMENT_VALUE rdfs:label ?ITEM_LABEL }}
        }}     
    '''
    
    result = wbi_core.ItemEngine.execute_sparql_query(query)
    wb_fields_df = []
    for bind in result['results']['bindings']:
        wb_fields_df.append({k: v['value'] for k, v in bind.items()})
    wb_fields_df = pd.DataFrame(wb_fields_df)        

    if 'STATEMENT_VALUE' in wb_fields_df.columns:
        wb_fields_df['STATEMENT_VALUE'] = wb_fields_df['STATEMENT_VALUE'].str.replace('http://wikibase.svc/entity/', '')
        
    return wb_fields_df    
    
    
def get_items_instance_of(P, Q):
    '''
    P - property "instance of"
    Q - parent item
    '''
    
    query = f'''
        SELECT ?item ?itemLabel 
            WHERE 
            {{
              ?item wdt:{P} wd:{Q}.
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            }}    
    '''
    result = wbi_core.ItemEngine.execute_sparql_query(query)
    
    df = []
    for bind in result['results']['bindings']:
        df.append({k: v['value'] for k, v in bind.items()})    
    df = pd.DataFrame(df)   
    
    if 'item' in df:
        df['item'] = df['item'].str.replace('http://wikibase.svc/entity/', '')
    
    return df
    
  
    
    
def gen_prop_dict(properties_list):
    return {i['label']: i['item'] \
        for _, i in get_items_by_label(properties_list, item_type = 'P').iterrows()}    
        
        
            
class WikiObject():
    def _fetch_statements(self):
        self.resolved_fields = self.df_input.copy()
        self.resolved_fields['QUALIFIER'] = f'API update'
        
        if not self.new_item:
            self.wb_statements['QUALIFIER'] = self.wb_statements['QUALIFIER'].fillna('Manual update') 
            self.wb_statements = self.wb_statements[~self.wb_statements.QUALIFIER.str.contains('API update')]
            self.resolved_fields = self.resolved_fields.append(self.wb_statements)
        
        
        self.df_repeated_statement = get_items_instance_of(P = self.properties_dict['global_statements_items']['located_in'], Q = self.Q)
        if self.df_repeated_statement.shape[0]:
            self.df_repeated_statement.rename(columns = {'item': 'STATEMENT_VALUE', 'itemLabel': 'ITEM_LABEL'}, inplace = True)
            self.df_repeated_statement['STATEMENT_TYPE'] = 'item'
            self.df_repeated_statement['STATEMENT_LABEL'] = self.repeated_statements
            self.df_repeated_statement['QUALIFIER'] = f'API update'
            self.resolved_fields = self.resolved_fields.append(self.df_repeated_statement)   
        return self.resolved_fields
    
    
    def _set_vars(self):
        Q_df = get_items_by_label([self.label], item_type = 'Q')
        self.Q_parent = get_items_by_label([self.parent_label], item_type = 'Q').at[0, 'item'] 
        self.wb_statements = pd.DataFrame(columns = ['STATEMENT_VALUE', 'QUALIFIER', 'STATEMENT_LABEL', 'STATEMENT_TYPE', 'ITEM_LABEL'])         
        
        if self.repeated_statements:
            self.properties_dict['statements'][self.repeated_statements] = self.properties_dict[self.repeated_statements]['P'] 
            
        if Q_df is None:  
            # Новый объект
            logging.info(f'No such object: {self.label}! New one will be created.')
            # assert self.df_input.shape[0] > 0 , 'Cannot create new item from empty input DataFrame!'
            
            self.Q = None
            self.new_item = True            
        else:
            # Существующий объект
            self.Q = Q_df.at[0, 'item']
            # _, self.Q_parent = None, None #get_wb_parent(self.Q, self.properties_dict['P'], self.login_instance)
            
            
            # Забрать состояние стейтментов с объекта
            # TO DO: плохо то, что если есть стейтмент не из PROPERTY_DICT , то он не будет забран
            # Да и вообще как-то громоздко получилось, это наверняка можно сделать одним запросом
            # self.wb_statements = pd.DataFrame()
            for label, P in self.properties_dict['statements'].items():
                state_i = get_wb_statements(
                    login_instance = self.login_instance, 
                    Q = self.Q, 
                    P = P,
                    Pq =  self.properties_dict['global_references']['Source']
                )
                state_i['STATEMENT_LABEL'] = label
                state_i['STATEMENT_TYPE'] = 'string'
                self.wb_statements = self.wb_statements.append(state_i)
                
                
            for label, P in self.properties_dict['global_statements_items'].items():
                state_i = get_wb_statements(
                    login_instance = self.login_instance, 
                    Q = self.Q, 
                    P = P,
                    Pq =  self.properties_dict['global_references']['Source']
                )
                state_i['STATEMENT_LABEL'] = label
                state_i['STATEMENT_TYPE'] = 'item'
                self.wb_statements = self.wb_statements.append(state_i)
                
            # А это, кажется, и не надо больше фетчить
#             if self.repeated_statements is not None:
#                 wb_repeated_statements = get_wb_statements(
#                     login_instance = self.login_instance, 
#                     Q = self.Q, 
#                     P = self.repeated_statements['P'],
#                     Pq =  self.properties_dict['global_references']['Source']
#                 )
#                 wb_repeated_statements['STATEMENT_LABEL'] = self.repeated_statement_label 
#                 wb_repeated_statements['STATEMENT_TYPE'] = 'item'
#                 self.wb_statements = self.wb_statements.append(wb_repeated_statements)    
                
            self.new_item = False
            
        
        self._fetch_statements()
        logging.info("""
            Object {label} (entity_id: {Q}), parent {parent_label} (entity_id: {Qp})
        """.format(
            label = self.label,
            Q = self.Q,
            parent_label = self.parent_label,
            Qp = self.Q_parent
        ))
        
        
    def push_to_wiki(self):
        its = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        update_qualifier_API = wbi_core.String(f'API update {its}', prop_nr=self.properties_dict['global_references']['Source'], 
                                is_qualifier = True)
        update_qualifier_manual = wbi_core.String('Manual update', prop_nr=self.properties_dict['global_references']['Source'], 
                                is_qualifier = True)
        
        # Общие технические поля
        if self.repeated_statements == 'DATABASE': #TO DO: придумать более красивый способне искать родителя для рутового айтема
            statements = []
        else:
            statements = [
                wbi_core.ItemID(self.properties_dict['global_company'], 
                        prop_nr=self.properties_dict['global_statements_items']['in_company'],
                        qualifiers=[update_qualifier_API]),
                wbi_core.ItemID(self.Q_parent, 
                        prop_nr=self.properties_dict['global_statements_items']['located_in'],
                        qualifiers=[update_qualifier_API])
            ]
        
        # Ручные поля, инпут, список детей
        for _, state_i in self.resolved_fields.iterrows():
            if state_i['QUALIFIER'] == 'API update':
                qualifier_i = update_qualifier_API
            else:
                qualifier_i = update_qualifier_manual
            
            if state_i['STATEMENT_TYPE'] == 'string':
                statements.append(
                    wbi_core.String(str(state_i['STATEMENT_VALUE']), prop_nr=self.properties_dict['statements'][state_i['STATEMENT_LABEL']], 
                                    qualifiers=[qualifier_i])
                )
            else:
                statements.append(
                    wbi_core.ItemID(str(state_i['STATEMENT_VALUE']), prop_nr=self.properties_dict['statements'][state_i['STATEMENT_LABEL']], 
                                    qualifiers=[qualifier_i])
                )            
        
        # Создание / Обновление айтема
        if self.new_item:
            item = wbi_core.ItemEngine(new_item=True, data=statements,core_props=set())
            item.set_label(self.label, if_exists='REPLACE')
        else:
            item = wbi_core.ItemEngine(new_item=False, item_id = self.Q, data=statements,core_props=set())
            
        self.write_responce = item.write(self.login_instance)    
        logging.info(self.write_responce)        
        
        
    def delete_from_wiki(self):
        wbi_core.ItemEngine.delete_item(self.Q, '', self.login_instance)
        
        
class WikiCompany(WikiObject): 
    def __init__(
        self, 
        label:str, 
        properties_dict:dict, 
        login_instance:wbi_login.Login, 
        parent_label:str,
        df_input = pd.DataFrame(columns = ['STATEMENT_VALUE', 'QUALIFIER', 'STATEMENT_LABEL', 'STATEMENT_TYPE', 'ITEM_LABEL'])
    ):        
        self.login_instance = login_instance
        self.df_input = df_input
        
        self.label = label
        self.parent_label = parent_label
        
        self.properties_dict = dict(
            properties_dict, **properties_dict['GLOBAL']
        )
        self.repeated_statements = 'DATABASE'
        
        self._set_vars()

                
class WikiDatabase(WikiObject): 
    def __init__(
        self, 
        label:str, 
        properties_dict:dict, 
        login_instance:wbi_login.Login, 
        parent_label:str,
        df_input = pd.DataFrame(columns = ['STATEMENT_VALUE', 'QUALIFIER', 'STATEMENT_LABEL', 'STATEMENT_TYPE', 'ITEM_LABEL'])
    ):        
        self.login_instance = login_instance
        self.df_input = df_input
        
        self.label = label
        self.parent_label = parent_label
        
        self.properties_dict = dict(
            properties_dict['DATABASE'], **properties_dict['GLOBAL']
        )
        self.repeated_statements = 'SCHEMA'
        
        self._set_vars()
                  
        
class WikiSchema(WikiObject): 
    def __init__(
        self, 
        label:str, 
        properties_dict:dict, 
        login_instance:wbi_login.Login, 
        df_input = pd.DataFrame(columns = ['STATEMENT_VALUE', 'QUALIFIER', 'STATEMENT_LABEL', 'STATEMENT_TYPE', 'ITEM_LABEL'])
    ):        
        self.login_instance = login_instance
        self.df_input = df_input
        
        self.label = label
        self.parent_label = '.'.join(label.split('.')[:-1])
        
        self.properties_dict = dict(
            properties_dict['DATABASE']['SCHEMA'], **properties_dict['GLOBAL']
        )
        self.repeated_statements = 'TABLE'
        
        self._set_vars()
        
        
class WikiTable(WikiObject):
    def __init__(
        self, 
        label:str, 
        properties_dict:dict, 
        login_instance:wbi_login.Login, 
        df_input = pd.DataFrame(columns = ['STATEMENT_VALUE', 'QUALIFIER', 'STATEMENT_LABEL', 'STATEMENT_TYPE', 'ITEM_LABEL'])
    ):        
        self.login_instance = login_instance
        self.df_input = df_input
        
        self.label = label
        self.parent_label = '.'.join(label.split('.')[:-1])
        
        self.properties_dict = dict(
            properties_dict['DATABASE']['SCHEMA']['TABLE'], **properties_dict['GLOBAL']
        )
        self.repeated_statements = 'COLUMN'
        
        self._set_vars()
        
        
class WikiColumn(WikiObject): 
    def __init__(
        self, 
        label:str, 
        properties_dict:dict, 
        login_instance:wbi_login.Login, 
        df_input = pd.DataFrame(columns = ['STATEMENT_VALUE', 'QUALIFIER', 'STATEMENT_LABEL', 'STATEMENT_TYPE', 'ITEM_LABEL'])
    ):        
        self.login_instance = login_instance
        self.df_input = df_input
        
        self.label = label
        self.parent_label = '.'.join(label.split('.')[:-1])
        
        self.properties_dict = dict(
            properties_dict['DATABASE']['SCHEMA']['TABLE']['COLUMN'], **properties_dict['GLOBAL']
        )
        self.repeated_statements = None
        
        self._set_vars()