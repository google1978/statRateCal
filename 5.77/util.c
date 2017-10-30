#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include "util.h"

int isLeapyear (int y) {

	int r = (y%400==0) || ((y%4==0) && (y%100!=0));	
	return r;
}

TDate getPreDate(TDate date) {

	TDate td=date;
	td.day--;
    if(td.day==0){

    	td.month--;
    	if(td.month==0){
    		td.day=31;
    		td.month=12;
    		td.year--;
    	}else{
    		switch(td.month){

    			case 1:
    			case 3:
    			case 5:
    			case 7:
    			case 8:
    			case 10:
    			    td.day=31;
    			    break;
    			case 4:
    			case 6:
    			case 9:
    			case 11:
    			    td.day=30; 
    			    break;
                case 2:
                    td.day=( isLeapyear(td.year) ? 29 : 28 );
                    break;
            }
        }
    }	

    return td;
}

TDate getCurrentDate() {

	TDate td;
	time_t rawtime;
	struct tm * timeinfo;
	time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    td.year = timeinfo->tm_year + 1900;
	td.month = timeinfo->tm_mon + 1;
	td.day = timeinfo->tm_mday;

	return td;

}
