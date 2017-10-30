#ifndef util__h
#define util__h

typedef struct st_Date {

	int year,month,day;

} TDate;

TDate getPreDate(TDate date);

TDate getCurrentDate();

#endif

