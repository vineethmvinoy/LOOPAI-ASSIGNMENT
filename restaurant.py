from trigger_report import generate_report
from displayResult import displayResult
from flask import Flask,jsonify
app=Flask(__name__)
@app.route('/get_report/<string:report_id>',methods=['GET']) #display the status of report generation and display report when completed
def get_report(report_id):
 finaldata=displayResult(report_id)
 return jsonify(finaldata)

@app.route('/trigger_report',methods=['GET'])    #trigger_report  function which initiates the report generation and return a report id to the user
def trigger_report():
  return generate_report(4) #The integer here return the specific amount of store reports
if __name__=="__main__":
  app.run(debug=True)