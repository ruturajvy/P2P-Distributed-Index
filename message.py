class message:
    '''Class that defines the message structure that is exchanged between server and client and between clients'''
    raw = ''           # raw string data
    mtype = ''         # specifies the message mtype 1.Register 2.Leave 3.PQuery 4.KeepAlive 5.RFCQuery 6.GetRFC 
    statuscode = ''   # specifies the status 1.OK 2.ERR
    hostname = ''      # specifies the destination host name / IP address
    headertag = ''
    headervalue = ''
    data = ''          # specifies the actual data to be sent

    def create_fields(self, raw_data):
        '''Creates fields from raw data'''
        self.raw = raw_data
        raw_words = raw_data.split('###')
        self.mtype = raw_words.pop(0)
        self.statuscode = raw_words.pop(0)
        self.hostname = raw_words.pop(0)
        self.headertag += raw_words.pop(0)
        self.headervalue += raw_words.pop(0)
        raw_words.remove('')        
        self.data = raw_words.pop(0)

    def create_raw(self):
        '''Function that converts the fields of the object into a single string and stores in self.raw'''
        sep = '###'
        self.raw = self.mtype + sep + self.statuscode + sep + self.hostname + sep + str(self.headertag) + sep + str(self.headervalue) + sep + sep +self.data