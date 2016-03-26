#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    DATA Capsules
        reads pickles
        adds metadata&logs to the dataframes pickled so we always have a summary
        of all operations (well defined goal)

        Will be the interface between the modules in the ETL process

        #TODO add group info + supp field to store additional misc. informations
        # might store many infos, such as comments on columns :
            ex : dc.capsule[col].log()
             or better dc.log_col['PPcustomerId']('rgihirg') : call a function stored in a dict by default ?
"""
import pickle
import pandas as pd
from collections import defaultdict
from time import strftime

def get_time() :
    return strftime('%Y-%m-%dT%H:%M:%S')
def get_env_info():
    return ""
#______________________________________________________________________________#
#object definition
#some inspiration from link :
#https://github.com/NathanEpstein/Dora/blob/master/Dora
import copy
from collections import defaultdict
import os.path
class DataCapsule(object) :
    """
        encapsulate : save the data with its associated informations
        decapsulate : (load the data after configurations), and show logs,
                      releasing the exhilarating scent of the data

        examples of what the capsule may contain :
            {
            'logs' : ['qw', 'qwe']
            'groups' : {'group1' : [1,2,3], 'group2' : [2,3,4] }
            'misc' : 'wdw'
            }

        ce qui est stocké comme attribut de l'objet est ce qui est courant à la session et
        qui contient des informations nécessaires pour le foncitonnement ds methodes

        ce qui est dans la capsule est
            -de l'information non necessaire pour la capsule mais utile
            pour d'autres taches
            -ou des informations passées anterieurement

            # TODO :
                - more precise time function, parse it and add as a column
                - set index with it sort and drop it...

    """
    def __init__(self, data = None, output = None):
        self.data = None
        self.data_accessor = None
        self.logs = []
        self.capsule = {}

        self.configure(data = data,
            output = output,
            log_verbosity = 1,
            reset_capsule = False,
            module_name= 'anonymous')

    def configure(self, data= None, output= None, log_verbosity= None,
            module_name= None, reset_capsule= None, log_summary= None,
             output_format= None, db_str = None):
        if (type(reset_capsule) is bool):
            self.reset_capsule = reset_capsule
        if (type(output) is str):
            output, ext =  os.path.splitext(output)
            self.output = output
            self.output_format= ext[1:] #remove point
            if self.output_format == '':
                self.output_format = 'capsule'
        if (type(data) is str):
            self.data_accessor = data
        if (type(data) is pd.DataFrame):
            self.data = data
        if (type(log_verbosity ) is int):
            self.log_verbosity = log_verbosity
        if (type(module_name) is str):
            self.module_name = module_name
        if (type(log_summary) is str):
            self.log_summary = log_summary
        if (type(output_format) is str):
            self.output_format = output_format #overrides previous format...
        if (type(db_str) is str):
            self.db_str= db_str

    def _load_pickle_(self, data_filename):
        data = pickle.load(file(data_filename))
        return data
    def _load_csv_(self, data_filename):
        # if need to load_csv using cutom parameters, load it manually and then give it to the constructor...
        data = pd.read_csv(data_filename)
        return data
    def _load_sql_(self, data_query):
        data = pd.read_sql(data_query, self.db_str) # TODO db_str might be in capsule... do conditional expr
        return data
    def _load_capsule_(self, data_filename):
        data = self._load_pickle_(data_filename)
        if isinstance(data, dict) :
            capsule = data
            data = capsule['data']
            if (type(data) is str):
                data = self._load_(data)
            else :
                data = capsule.pop('data')
            if not self.reset_capsule:
                self.capsule.update(capsule)
        else :
            print 'Probably a dataframe...'
        return data
    def _load_(self, data_accessor):
        d = {
            'csv': self._load_csv_,
            'pickle': self._load_pickle_,
            'sql': self._load_sql_,
            'capsule': self._load_capsule_
            }

        ext =  os.path.splitext(data_accessor)[1][1:] #move this to input format
        return d[ext](data_accessor)
    def decapsulate(self):
        if (not self.data) and (self.data_accessor) :
            self.data = self._load_(self.data_accessor)
        return self.get_logs(log_verbosity= self.log_verbosity)

    def get_logs(self, logs = None, log_verbosity =1):
        if logs is None:
            try:
                logs = self.capsule['logs']
            except KeyError:
                return
        df_logs = pd.DataFrame.from_dict(logs, orient = 'columns')
        if len(df_logs) == 0 :
            return
        if log_verbosity == 1 :
            df_logs.drop(['sub_log', 'T'], axis= 1, inplace= True)
            df_logs.drop_duplicates(inplace = True)
            df_logs.set_index(['date', 'module_name','log'], inplace =True)
        else :
            #df_logs['log_date'] = df_logs['log_date'].apply(lambda x: x.strftime('%H-%M'))
            df_logs['T'] = df_logs['T'].apply(lambda x: x.split('T')[1])
            df_logs.set_index(['date', 'module_name','log','T'], inplace =True)
        df_logs.sort_index(inplace = True, ascending = False, sort_remaining = True)
        return df_logs
    def log(self, m):
        self.logs.append((get_time(),str(m)))

    def encapsulate(self, data = None):
        #TODO find a better way to store logs... too much duplication
        if self.output_format == '' or self.output_format not in  ['csv', 'sql','pickle', 'capsule'] :
            self.output_format = 'capsule'

        res_capsule = copy.deepcopy(self.capsule) # does not contain  THE data
        try :
            res_capsule['logs']
        except KeyError:
            res_capsule['logs'] = defaultdict(list)
        if self.logs == []:
            self.logs = [(get_time(),'')]
        for date, log in self.logs :
            res_capsule['logs']['module_name'].append( self.module_name )
            res_capsule['logs']['T'].append( date )
            res_capsule['logs']['log'].append( self.log_summary )
            res_capsule['logs']['sub_log'].append( log )
            res_capsule['logs']['date'].append( get_time() ) #time of creation of capsule
        res_capsule['env_info'] = get_env_info() # last envt info

        data = data if type(data) != type(None) else self.data
        self._save_( res_capsule, data )
    def _save_(self, res_capsule, data):
        d = {
            'csv': self._save_csv_,
            'pickle': self._save_pickle_,
            'sql': self._save_sql_
            }
        if self.output_format == 'capsule':
            res_capsule['data'] = data
        else :
            d[self.output_format](self.output+'.'+self.output_format, data)
            res_capsule['data'] = self.output+'.'+self.output_format

        #store the capsule anyway
        self._save_pickle_(self.output+'.capsule', res_capsule)
    def _save_csv_(self,filename, data):
        data.to_csv(filename)
    def _save_pickle_(self,filename, data):
        pickle.dump(data, file(filename, 'w'))
    def _save_sql_(self):
        #TODO
        pass

    def reset_capsule(self, reset_log= None, reset_misc= None):
        self.capsule = {}
    def reset_current_logs(self):
        self.logs = []
    def reset_capsule_logs(self):
        self.capsule['logs'] = []
    def reset_all_logs(self,):
        self.reset_current_logs()
        self.reset_capsule_logs()
    @staticmethod
    def merge(capsules = None, data=None, output=None) :
        res = DataCapsule(data = data, output= output)
        res.capsule['logs'] = defaultdict(list)
        for dc in capsules:
            try:
                dc = copy.deepcopy(dc)
                logs = dc.capsule.pop('logs')
                for k, v in logs.iteritems() :
                    res.capsule['logs'][k].extend(v)
            except KeyError:
                pass
            res.capsule.update(dc.capsule) # hope there is no damaging collision
        return res

#______________________________________________________________________________#
import fnmatch
from IPython.display import display
class TitleFilename(object):
    def __init__(self, value, filename, bar =False):
        self.value = value
        self.filename = filename
        self.bar = '<hr>' if bar else ''
    def _repr_html_(self):
        base_path, ext =  os.path.splitext(self.filename)
        return "<p><em> {0}<strong>  {1}</strong> {2}</em></p>{3}".format(self.value, base_path, ext, self.bar)

def parse_datasets (path, log_verbosity =1):
    for root, dirnames, filenames in os.walk(path):
        for filename in sorted(fnmatch.filter(filenames, '*.capsule')):
            display(TitleFilename('DataCapsule',filename) )
            capsule_raw = pickle.load(file(filename))
            display(DataCapsule().get_logs(logs =capsule_raw['logs'], log_verbosity = log_verbosity))
            if type(capsule_raw['data']) is str :
                display(TitleFilename('Data stored in :',capsule_raw['data'],bar = True))
            else :
                display(TitleFilename('Data stored in capsule',"",bar = True))
