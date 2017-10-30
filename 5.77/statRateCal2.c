#include <stdio.h>
#include <pthread.h> 
#include <stdlib.h>  
#include <string.h>

#include "ocilib.h"
#include "statRateCal.h"
#include "util.h"

void * getDataFromGeneralDB(void *arg);

void saveDRData(Res_RateCalu *arg);

void err_handler(OCI_Error *err) {
	printf("code  : ORA-%05i\n" 
		   "msg   : %s\n"                 
		   "sql   : %s\n",                 
		   OCI_ErrorGetOCICode(err),                  
		   OCI_ErrorGetString(err),                 
		   OCI_GetSql(OCI_ErrorGetStatement(err))            
		   ); 
}

void print_result(Res_RateCalu *res) {

	printf("startDate: %s\n",res->_startDate);
	printf("endDate: %s\n",res->_endDate);
	
	printf("One Hour DoorToDoor Rate is: %s [%d/%d]\n",res->_qualifiedRateDtoD,res->_qualNumDtoD,res->_totalNumDtoD);
    printf("Half An Hour Group Truck DoorToDoor Rate is: %s [%d/%d]\n",res->_groupHalfRateDtoD,res->_groupQualNumDtoD,res->_groupNumDtoD);
    
	printf("One Hour DoorToInst Rate is: %s [%d/%d]\n",res->_qualifiedRateDtoI,res->_qualNumDtoI,res->_totalNumDtoI);
    printf("Half An Hour Group Truck DoorToInst Rate is: %s [%d/%d]\n",res->_groupHalfRateDtoI,res->_groupQualNumDtoI,res->_groupNumDtoI);

}

void saveDRData(Res_RateCalu *arg) {

    OCI_Connection *conn;
	conn = OCI_ConnectionCreate("EDI_32", "edi", "edi2014opend", OCI_SESSION_DEFAULT);
    OCI_Statement  *st;
    st = OCI_StatementCreate(conn);
    
    OCI_Prepare(st, OTEXT("begin ")
        OTEXT("  SP_UPDATE_DR_DATA")
        OTEXT("(:vTerminal,:vDate,:vTotalNum,:vGroupNum,:vQualNumDtoD,:vGroupQualNumDtoD,")
        OTEXT(":vQualNumDtoI,:vGroupQualNumDtoI); ")
        OTEXT("end; "));

    OCI_BindString(st, ":vTerminal", arg->_terminal, 16);
    OCI_BindString(st, ":vDate", arg->_startDate, 16);

    OCI_BindInt(st, ":vTotalNum", &arg->_totalNumDtoD);
    OCI_BindInt(st, ":vGroupNum", &arg->_groupNumDtoD);
    OCI_BindInt(st, ":vQualNumDtoD", &arg->_qualNumDtoD);
    OCI_BindInt(st, ":vGroupQualNumDtoD", &arg->_groupQualNumDtoD);
    OCI_BindInt(st, ":vQualNumDtoI", &arg->_qualNumDtoI);
    OCI_BindInt(st, ":vGroupQualNumDtoI", &arg->_groupQualNumDtoI);
    OCI_Execute(st);

}

int main(int argc, char** argv) {

    pthread_t t_yzct;
    pthread_t t_blctms;

	Res_RateCalu * res_cal_yzct = (Res_RateCalu *)malloc(sizeof(Res_RateCalu));
	Res_RateCalu * res_cal_blctms = (Res_RateCalu *)malloc(sizeof(Res_RateCalu));

    strcpy(res_cal_yzct->_terminal, "YZCT");
    strcpy(res_cal_blctms->_terminal, "BLCTMS");

	if (!OCI_Initialize(NULL, NULL, OCI_ENV_DEFAULT | OCI_ENV_THREADED))
        return -1;
	
	if (argc > 3) {

        printf("parameter number error!\n");
        return -1;
    }
    
    if (argc == 3) {
    	
    	strcpy(res_cal_yzct->_startDate, argv[1]);
    	strcpy(res_cal_yzct->_endDate, argv[2]);
    	

        strcpy(res_cal_blctms->_startDate, argv[1]);
    	strcpy(res_cal_blctms->_endDate, argv[2]);
    	
    }

    if (argc == 2) {
    	
    	strcpy(res_cal_yzct->_startDate, argv[1]);
    	strcpy(res_cal_yzct->_endDate, argv[1]);
    	

        strcpy(res_cal_blctms->_startDate, argv[1]);
    	strcpy(res_cal_blctms->_endDate, argv[1]);
    	
    }

    if (argc == 1) {

    	TDate yesterday;
    	yesterday = getPreDate(getCurrentDate());
    
        char startDate[16];
        char endDate[16];
        sprintf(startDate, "%04d%02d%02d",yesterday.year,yesterday.month,yesterday.day);  
        sprintf(endDate, "%04d%02d%02d",yesterday.year,yesterday.month,yesterday.day); 

        strcpy(res_cal_yzct->_startDate, startDate);
    	strcpy(res_cal_yzct->_endDate, endDate);
    	

        strcpy(res_cal_blctms->_startDate, startDate);
    	strcpy(res_cal_blctms->_endDate, endDate);

    }

    strcpy(res_cal_yzct->_dbName, "YZCTDB2");
    strcpy(res_cal_yzct->_dbUser, "hostdb");
    strcpy(res_cal_yzct->_dbPassword, "hostdb");

    strcpy(res_cal_blctms->_dbName, "MSCTOS2");
    strcpy(res_cal_blctms->_dbUser, "system");
    strcpy(res_cal_blctms->_dbPassword, "oracle");

    pthread_create(&t_yzct,NULL,getDataFromGeneralDB,res_cal_yzct);

    pthread_create(&t_blctms,NULL,getDataFromGeneralDB,res_cal_blctms);

    pthread_join(t_yzct,NULL);

    pthread_join(t_blctms,NULL);

    if (argc == 3) {

    	printf("--------------yzct---------------\n");
    	print_result(res_cal_yzct);

        printf("--------------blctms---------------\n");
        print_result(res_cal_blctms);

	}

	if ((argc == 1) || (argc == 2)) {

    	saveDRData(res_cal_yzct);
    	saveDRData(res_cal_blctms);
    }
    

	free(res_cal_yzct);
	free(res_cal_blctms);
    OCI_Cleanup();
    return 0;

}

void * getDataFromGeneralDB(void *arg) {

	Res_RateCalu * context = (Res_RateCalu *)arg;
	OCI_Connection *conn;

	char _totalNum[16];
	char _errSign[8];
	char _errDesc[1024];
	char _qualified_Num[16];
	char _group_Num[16];
	char _group_half_Num[16];

    conn = OCI_ConnectionCreate(context->_dbName, context->_dbUser, context->_dbPassword, OCI_SESSION_DEFAULT);
    OCI_Statement  *st;
    st = OCI_StatementCreate(conn);
    
    OCI_Prepare(st, OTEXT("begin ")
    OTEXT("  SP_STATISTICS_QUALIFIED_RATE")
    OTEXT("(:vStart_Time,:vEnd_Time,:vTotalNum,:vQualified_Num,:vErrSign,:vErrDesc,:vQualified_Rate,")
    OTEXT(":vGroup_half_rate,:vGroup_Num,:vGroup_half_Num); ")
    OTEXT("end; "));
                
    OCI_BindString(st, ":vStart_Time", context->_startDate, 16);
    OCI_BindString(st, ":vEnd_Time", context->_endDate, 16);

    OCI_BindString(st, ":vTotalNum", _totalNum, 16);
    OCI_BindString(st, ":vQualified_Num", _qualified_Num, 16);
    OCI_BindString(st, ":vErrSign", _errSign, 8);
    OCI_BindString(st, ":vErrDesc", _errDesc, 1024);
    OCI_BindString(st, ":vQualified_Rate", context->_qualifiedRateDtoD, 16);
    OCI_BindString(st, ":vGroup_half_rate", context->_groupHalfRateDtoD, 16);
    OCI_BindString(st, ":vGroup_Num", _group_Num, 16);
    OCI_BindString(st, ":vGroup_half_Num", _group_half_Num, 16);
    OCI_Execute(st);
    
    context->_totalNumDtoD = atoi(_totalNum);
    context->_qualNumDtoD = atoi(_qualified_Num);
    context->_groupNumDtoD = atoi(_group_Num);
    context->_groupQualNumDtoD = atoi(_group_half_Num);

    OCI_Prepare(st, OTEXT("begin ")
    OTEXT("  SP_STATISTICS_QUALIFIED_RATE2")
    OTEXT("(:vStart_Time,:vEnd_Time,:vTotalNum,:vQualified_Num,:vQualified_Rate,")
    OTEXT(":vGroup_Num,:vGroup_half_Num,:vGroup_half_rate,:vErrSign,:vErrDesc); ")
    OTEXT("end; "));
                
    OCI_BindString(st, ":vStart_Time", context->_startDate, 16);
    OCI_BindString(st, ":vEnd_Time", context->_endDate, 16);

    OCI_BindString(st, ":vTotalNum", _totalNum, 16);
    OCI_BindString(st, ":vQualified_Num", _qualified_Num, 16);
    OCI_BindString(st, ":vErrSign", _errSign, 8);
    OCI_BindString(st, ":vErrDesc", _errDesc, 1024);
    OCI_BindString(st, ":vQualified_Rate", context->_qualifiedRateDtoI, 16);
    OCI_BindString(st, ":vGroup_half_rate", context->_groupHalfRateDtoI, 16);
    OCI_BindString(st, ":vGroup_Num", _group_Num, 16);
    OCI_BindString(st, ":vGroup_half_Num", _group_half_Num, 16);
    OCI_Execute(st);
    
    context->_totalNumDtoI = atoi(_totalNum);
    context->_qualNumDtoI = atoi(_qualified_Num);
    context->_groupNumDtoI = atoi(_group_Num);
    context->_groupQualNumDtoI = atoi(_group_half_Num);

    OCI_StatementFree(st);
    OCI_ConnectionFree(conn);

    return ((void*)EXIT_SUCCESS);

}


