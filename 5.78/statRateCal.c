#include <stdio.h>
#include <pthread.h> 
#include <stdlib.h>  
#include <string.h>

#include "ocilib.h"
#include "statRateCal.h"
#include "util.h"

void getDataFromGeneralDB(OCI_Thread *thread,void *arg);

void getDataFromTMSPRD(OCI_Thread *thread,void *arg);

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

    OCI_Thread *t_blct2;
    OCI_Thread *t_blct3;
    OCI_Thread *t_blct;

	Res_RateCalu * res_cal_blct2 = (Res_RateCalu *)malloc(sizeof(Res_RateCalu));
	Res_RateCalu * res_cal_blct3 = (Res_RateCalu *)malloc(sizeof(Res_RateCalu));
	//Res_RateCalu * res_cal_blctyd = (Res_RateCalu *)malloc(sizeof(Res_RateCalu));
	Res_RateCalu * res_cal_blct = (Res_RateCalu *)malloc(sizeof(Res_RateCalu));

    strcpy(res_cal_blct2->_terminal, "BLCT2");
    strcpy(res_cal_blct3->_terminal, "BLCT3");
    //strcpy(res_cal_blctyd->_terminal, "BLCTYD");
    strcpy(res_cal_blct->_terminal, "BLCT");

       
	if (!OCI_Initialize(err_handler, NULL, OCI_ENV_DEFAULT | OCI_ENV_THREADED))
        return -1;
	
	if (argc > 3) {

        printf("parameter number error!\n");
        return -1;
    }
    
    if (argc == 3) {
    	
    	strcpy(res_cal_blct2->_startDate, argv[1]);
    	strcpy(res_cal_blct2->_endDate, argv[2]);
    	

        strcpy(res_cal_blct3->_startDate, argv[1]);
    	strcpy(res_cal_blct3->_endDate, argv[2]);
    	
    	
    	//strcpy(res_cal_blctyd->_startDate, argv[1]);
    	//strcpy(res_cal_blctyd->_endDate, argv[2]);
    	

    	strcpy(res_cal_blct->_startDate, argv[1]);
    	strcpy(res_cal_blct->_endDate, argv[2]);
    	
    }

    if (argc == 2) {
    	
    	strcpy(res_cal_blct2->_startDate, argv[1]);
    	strcpy(res_cal_blct2->_endDate, argv[1]);
    	

        strcpy(res_cal_blct3->_startDate, argv[1]);
    	strcpy(res_cal_blct3->_endDate, argv[1]);
    	
    	
    	//strcpy(res_cal_blctyd->_startDate, argv[1]);
    	//strcpy(res_cal_blctyd->_endDate, argv[1]);
    	

    	strcpy(res_cal_blct->_startDate, argv[1]);
    	strcpy(res_cal_blct->_endDate, argv[1]);
    	
    }

    if (argc == 1) {

    	TDate yesterday;
    	yesterday = getPreDate(getCurrentDate());
    
        char startDate[16];
        char endDate[16];
        sprintf(startDate, "%04d%02d%02d",yesterday.year,yesterday.month,yesterday.day);  
        sprintf(endDate, "%04d%02d%02d",yesterday.year,yesterday.month,yesterday.day); 

        strcpy(res_cal_blct2->_startDate, startDate);
    	strcpy(res_cal_blct2->_endDate, endDate);
    	

        strcpy(res_cal_blct3->_startDate, startDate);
    	strcpy(res_cal_blct3->_endDate, endDate);
    	
    	
    	//strcpy(res_cal_blctyd->_startDate, startDate);
    	//strcpy(res_cal_blctyd->_endDate, endDate);
    	

    	strcpy(res_cal_blct->_startDate, startDate);
    	strcpy(res_cal_blct->_endDate, endDate); 

    }

    strcpy(res_cal_blct2->_dbName, "B2CTDB");
    strcpy(res_cal_blct2->_dbUser, "system");
    strcpy(res_cal_blct2->_dbPassword, "oracle");

    strcpy(res_cal_blct3->_dbName, "BLCT3DB");
    strcpy(res_cal_blct3->_dbUser, "b3csszz");
    strcpy(res_cal_blct3->_dbPassword, "b3csszz");

    //strcpy(res_cal_blctyd->_dbName, "C5CDB");
    //strcpy(res_cal_blctyd->_dbUser, "system");
    //strcpy(res_cal_blctyd->_dbPassword, "oracle");

    strcpy(res_cal_blct->_dbName, "TMSPRD");
    strcpy(res_cal_blct->_dbUser, "system");
    strcpy(res_cal_blct->_dbPassword, "oracle");

    t_blct2 = OCI_ThreadCreate();
    t_blct3 = OCI_ThreadCreate();
    t_blct = OCI_ThreadCreate();

    OCI_ThreadRun(t_blct2, getDataFromGeneralDB, res_cal_blct2);

    OCI_ThreadRun(t_blct3, getDataFromGeneralDB, res_cal_blct3);

    OCI_ThreadRun(t_blct, getDataFromTMSPRD, res_cal_blct);

    OCI_ThreadJoin(t_blct2);
    OCI_ThreadFree(t_blct2);

    OCI_ThreadJoin(t_blct3);
    OCI_ThreadFree(t_blct3);

    OCI_ThreadJoin(t_blct);
    OCI_ThreadFree(t_blct);

    

    if (argc == 3) {

    	printf("--------------blct2---------------\n");
    	print_result(res_cal_blct2);

        printf("--------------blct---------------\n");
        print_result(res_cal_blct);

        printf("--------------blct3---------------\n");
        print_result(res_cal_blct3);

        //printf("--------------blctyd---------------\n");
        //print_result(res_cal_blctyd);

        //res_cal_blct3->_totalNumDtoD = res_cal_blct3->_totalNumDtoD + res_cal_blctyd->_totalNumDtoD;
    	//res_cal_blct3->_groupNumDtoD = res_cal_blct3->_groupNumDtoD + res_cal_blctyd->_groupNumDtoD;
    	//res_cal_blct3->_qualNumDtoD = res_cal_blct3->_qualNumDtoD + res_cal_blctyd->_qualNumDtoD;
    	//res_cal_blct3->_qualNumDtoI = res_cal_blct3->_qualNumDtoI + res_cal_blctyd->_qualNumDtoI;
    	//res_cal_blct3->_groupQualNumDtoD = res_cal_blct3->_groupQualNumDtoD + res_cal_blctyd->_groupQualNumDtoD;
    	//res_cal_blct3->_groupQualNumDtoI = res_cal_blct3->_groupQualNumDtoI + res_cal_blctyd->_groupQualNumDtoI;



    	//printf("--------------blct3---------------\n");

    	//printf("startDate: %s\n",res_cal_blct3->_startDate);
	    //printf("endDate: %s\n",res_cal_blct3->_endDate);
	
	    //printf("One Hour DoorToDoor Rate is: %4.2f%% [%d/%d]\n", 100.0 * res_cal_blct3->_qualNumDtoD / res_cal_blct3->_totalNumDtoD,
        //    res_cal_blct3->_qualNumDtoD,res_cal_blct3->_totalNumDtoD );
        //printf("Half An Hour Group Truck DoorToDoor Rate is: %4.2f%% [%d/%d]\n", 100.0 * res_cal_blct3->_groupQualNumDtoD / res_cal_blct3->_groupNumDtoD,
        //    res_cal_blct3->_groupQualNumDtoD,res_cal_blct3->_groupNumDtoD);
    
	    //printf("One Hour DoorToInst Rate is: %4.2f%% [%d/%d]\n", 100.0 * res_cal_blct3->_qualNumDtoI / res_cal_blct3->_totalNumDtoD,
        //    res_cal_blct3->_qualNumDtoI,res_cal_blct3->_totalNumDtoD);
        //printf("Half An Hour Group Truck DoorToInst Rate is: %4.2f%% [%d/%d]\n", 100.0 * res_cal_blct3->_groupQualNumDtoI / res_cal_blct3->_groupNumDtoD,
        //    res_cal_blct3->_groupQualNumDtoI,res_cal_blct3->_groupNumDtoD);
    	

	}

	if ((argc == 1) || (argc == 2)) {

    	saveDRData(res_cal_blct2);
    	saveDRData(res_cal_blct);

    	//res_cal_blct3->_totalNumDtoD = res_cal_blct3->_totalNumDtoD + res_cal_blctyd->_totalNumDtoD;
    	//res_cal_blct3->_groupNumDtoD = res_cal_blct3->_groupNumDtoD + res_cal_blctyd->_groupNumDtoD;
    	//res_cal_blct3->_qualNumDtoD = res_cal_blct3->_qualNumDtoD + res_cal_blctyd->_qualNumDtoD;
    	//res_cal_blct3->_qualNumDtoI = res_cal_blct3->_qualNumDtoI + res_cal_blctyd->_qualNumDtoI;
    	//res_cal_blct3->_groupQualNumDtoD = res_cal_blct3->_groupQualNumDtoD + res_cal_blctyd->_groupQualNumDtoD;
    	//res_cal_blct3->_groupQualNumDtoI = res_cal_blct3->_groupQualNumDtoI + res_cal_blctyd->_groupQualNumDtoI;

    	saveDRData(res_cal_blct3);

    }
    

	free(res_cal_blct2);
	free(res_cal_blct3);
	//free(res_cal_blctyd);
	free(res_cal_blct);
    OCI_Cleanup();
    return 0;

}

void getDataFromGeneralDB(OCI_Thread *thread,void *arg) {

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
    
    printf("fuck you BLCT2!!!!\n");    
     
    context->_totalNumDtoI = atoi(_totalNum);
    context->_qualNumDtoI = atoi(_qualified_Num);
    context->_groupNumDtoI = atoi(_group_Num);
    context->_groupQualNumDtoI = atoi(_group_half_Num);

    OCI_StatementFree(st);
    OCI_ConnectionFree(conn);

    //return ((void*)EXIT_SUCCESS);

}

void getDataFromTMSPRD(OCI_Thread *thread,void *arg) {

	Res_RateCalu * context = (Res_RateCalu *)arg;
	OCI_Connection *conn;

	char _totalNum[16];
	char _errSign[8];
	char _errDesc[1024];
	char _qualified_NumDtoD[16];
	char _group_Num[16];
	char _group_half_NumDtoD[16];
	char _qualified_NumDtoI[16];
	char _group_half_NumDtoI[16];

    conn = OCI_ConnectionCreate(context->_dbName, context->_dbUser, context->_dbPassword, OCI_SESSION_DEFAULT);
    OCI_Statement  *st;
    st = OCI_StatementCreate(conn);
    
    OCI_Prepare(st, OTEXT("begin ")
    OTEXT("  SP_STATISTICS_QUALIFIED_RATE")
    OTEXT("(:vStart_Time,:vEnd_Time,:vOne_Hour_Rate_DtoD,:vOne_Hour_Rate_DtoI,:vGroup_Half_Rate_DtoD,:vGroup_Half_Rate_DtoI,")
    OTEXT(":vErr_Sign,:vErr_Desc,:vTotal_Num,:vGroup_Num,:vOne_Num_DtoD,:vOne_Num_DtoI,:vGroup_half_Num_DtoD,:vGroup_Half_Num_DtoI); ")
    OTEXT("end; "));
                
    OCI_BindString(st, ":vStart_Time", context->_startDate, 16);
    OCI_BindString(st, ":vEnd_Time", context->_endDate, 16);

    OCI_BindString(st, ":vTotal_Num", _totalNum, 16);
    OCI_BindString(st, ":vGroup_Num", _group_Num, 16);
    OCI_BindString(st, ":vErr_Sign", _errSign, 8);
    OCI_BindString(st, ":vErr_Desc", _errDesc, 1024);
    OCI_BindString(st, ":vOne_Hour_Rate_DtoD", context->_qualifiedRateDtoD, 16);
    OCI_BindString(st, ":vGroup_Half_Rate_DtoD", context->_groupHalfRateDtoD, 16);
    OCI_BindString(st, ":vOne_Hour_Rate_DtoI", context->_qualifiedRateDtoI, 16);
    OCI_BindString(st, ":vGroup_Half_Rate_DtoI", context->_groupHalfRateDtoI, 16);
    OCI_BindString(st, ":vOne_Num_DtoD", _qualified_NumDtoD, 16);
    OCI_BindString(st, ":vOne_Num_DtoI", _qualified_NumDtoI, 16);
    OCI_BindString(st, ":vGroup_half_Num_DtoD", _group_half_NumDtoD, 16);
    OCI_BindString(st, ":vGroup_Half_Num_DtoI", _group_half_NumDtoI, 16);
    OCI_Execute(st);
    printf("fuck you BLCT !!!! \n");
    context->_totalNumDtoD = atoi(_totalNum);
    context->_qualNumDtoD = atoi(_qualified_NumDtoD);
    context->_groupNumDtoD = atoi(_group_Num);
    context->_groupQualNumDtoD = atoi(_group_half_NumDtoD);
    context->_totalNumDtoI = atoi(_totalNum);
    context->_qualNumDtoI = atoi(_qualified_NumDtoI);
    context->_groupNumDtoI = atoi(_group_Num);
    context->_groupQualNumDtoI = atoi(_group_half_NumDtoI);

    OCI_StatementFree(st);
    OCI_ConnectionFree(conn);
    //return ((void*)EXIT_SUCCESS);

}  


