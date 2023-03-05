import requests
import config
from utility_function import   calculate_hour_minute , convert_list_dict_to_df
import time 
import pandas as pd
from datetime import timedelta, date
import logs.log_config

import logging
flight_search_logger = logging.getLogger(__name__)


def extractdata_and_store_response_into_df(response):
    """ 
        Iterating and extract the flight information from the response .
        Adding the data into list of dictionary .
    """
    try:
        flight_list = []
        for joury_list in response['Search']['FlightDataList']['JourneyList']:
            for details in joury_list:
                flight_list = get_details_flights_into_list_dict(details , flight_list)

        """ calling the function to convert Convert a List of Dictionaries into df and  remove the duplicates """
        df = convert_list_dict_to_df(flight_list)
         
        return {
            "Status" : "Success" ,
            "Result" : df
        }
    
    except Exception as e:
        return  {
            "Status" : "Failure" ,
            "Message": f"Exception while getting the response : {e}"
        }


def get_details_flights_into_list_dict(details , flight_list):
    """ 
        Passing the details of the flights ,
        extract the required information from the details add those data into the dictionary and append into the list
        return  the list of dictionary 
    """
    flight_details = {}
    
    for flightlist in details['FlightDetails']['Details']:
        if len(flightlist) == 1:
            flight_details['stops'] = "non stop"

            ##### flight idetails 
            from_detail = flightlist[0]['Origin']
            Destination = flightlist[0]['Destination']

            flight_details['Source'] =  from_detail['AirportCode']
            flight_details['Destination'] =  Destination['AirportCode']
            flight_details['Source City'] =  from_detail['CityName']
            flight_details['Destination City'] =  Destination['CityName']
            flight_details['Flight Company'] = flightlist[0]['OperatorName']
            flight_details['Flight Number'] = flightlist[0]['FlightNumber']
            flight_details['Flight Code'] = flightlist[0]['OperatorCode']

            flight_details['Departure Date'] = from_detail['DateTime'].split(' ')[0]
            flight_details['Departure Time'] = from_detail['DateTime'].split(' ')[1]
            flight_details['Arrival Date'] = Destination['DateTime'].split(' ')[0]
            flight_details['Arrival Time'] = Destination['DateTime'].split(' ')[1]

            ### calculating duration : pass the minutes as parameter to the function
            flight_details['Duration'] = calculate_hour_minute(flightlist[0]['Duration']) 

            ##### getting the price details
            flight_details['Currency'] = details['Price']['Currency']
            flight_details['Base Price'] =  details['Price']['PassengerBreakup']['ADT']['BasePrice']
            flight_details['Tax'] = details['Price']['PassengerBreakup']['ADT']['Tax']
            flight_details['Total Price'] =  details['Price']['PassengerBreakup']['ADT']['TotalPrice']
            flight_details['Pasanger Count'] = details['Price']['PassengerBreakup']['ADT']['PassengerCount']

            flight_details['Executed Date'] = date.today()
            flight_list.append(flight_details)

    return flight_list


def get_response_from_url(from_city , to_city , DepartureDate):
    """
        Call the search url for the request_data , once the response get from the request
        call the function to extract theresponse data into the dataframe .
    """
    try:
        url = config.search_url
        headers = config.headers
        request_data  = {
                    "AdultCount": "1",
                    "ChildCount": "0",
                    "InfantCount": "0",
                    "JourneyType": "OneWay",
                    "PreferredAirlines": [
                        ""
                    ],
                    "CabinClass": "Economy",
                    "Segments": [{
                        "Origin": from_city ,
                        "Destination": to_city,
                        "DepartureDate": f"{str(DepartureDate)}T00:00:00"
                    }]  
                }
        response = requests.get(url ,headers=headers , json=request_data)

        """ If the response status_code == 200 continue else return the error message """
        if response.status_code == 200:
            flight_search_logger.info(f"Search api response for {from_city} to {to_city} fetched successfully")
            res = response.json()
            df_res = extractdata_and_store_response_into_df(res)

            return {
                "Status" :"Success" ,
                "Message" : "Succesfully get the response and added into dataframe",
                "Result" : df_res['Result']
            } 

        else:
            flight_search_logger.error(f"Error while get the response from the search url : {response.text}")
            return {
                "Status" : "Failure" ,
                "Message" : f"{response.text}"
            }
        
    except Exception as e:
        flight_search_logger.error(f"Exception while fetching the response from the search resquest :- {e}")
        return {
                "Status" : "Failure" ,
                "Message" : f"Exception while fetching the response from the search resquest :- {e}"
        }


def append_response_into_list(days  , dataframe_list):
    """
        loop through all the cities and get the response by calling the function,
        and store the resullt into the list
    """
    try:
        all_cities = config.all_cities

        for from_city in all_cities:
            for to_city in all_cities:
                flight_search_logger.info(f"From City -- {from_city} To city -- {to_city}")
                DepartureDate = date.today() + timedelta(days=days)

                """ If the from_city is not equal to to_city then exceute the below code """
                if from_city != to_city:
                    res = get_response_from_url(from_city=from_city , to_city=to_city , DepartureDate=DepartureDate)
                    if res['Status'] == "Success":
                        #### append all the response into the dataframe list
                        res['Result']['days'] = days
                        dataframe_list.append(res['Result'])

                    else:
                        flight_search_logger.error(res['Message'])
                        return {
                            "Status" : "Failure" ,
                            "Message" : res['Message']
                        }
                    
        """ finallly send the dataframe list """            
        return {
            "Status" :"Success" ,
            "Result" : dataframe_list
        }

    except Exception as e:
        flight_search_logger.error(f"Exception while appending the results into list : {e}")
        return {
            "Status" : "Failure" ,
            "Message" : f"Exception while appending the results into list : {e}"
        }


def get_flight_information():
    """ 
        Function is used to fetch the flight information from source to the destination metro cities 
        and storing the information into the file
    """
    try:
        dataframe_list = []
        for days in config.FORWARDS_DAYS:
            flight_search_logger.info(f"Getting the response for the {days} day from the today - {date.today()}")
            search_res = append_response_into_list(days = days , dataframe_list=dataframe_list)

            """ If the response status is failure return the error message else continue to fetch the other response """
            if search_res['Status'] != "Success" :
                flight_search_logger.error(search_res['Message'])
                return search_res

        """ Merge the dataframe and store the dataframe into the csvfile"""
        data_f = pd.concat(dataframe_list)

        """  writing a csv file """ 
        res = data_f.to_csv('Flight_Info.csv' , index=False)

        flight_search_logger.info("Sucessfully Stored the information into CSV file")
        return {
            "Status" :"Success",
            "Message" :"Sucessfully Stored the information into CSV file"
        }

    except Exception as e:
        flight_search_logger.error(f"{e}")
        context = {
            "Status" :"Failure" ,
            "Message" :f"Exception while getting the flight information :- {e}" 
        }
        return context



if __name__ == "__main__":
    flight_search_logger.info("Program Execution Starts")
    start_time = time.time()

    """Call the function to get the flight information """
    result = get_flight_information()
    flight_search_logger.info(f"Final Result :- {result}")

    end_time = time.time()
    total_time = end_time - start_time

    flight_search_logger.info(f"Total time taken for execution: {total_time}")
    flight_search_logger.info("Program Execution Ends")
