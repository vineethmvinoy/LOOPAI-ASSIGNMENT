from trigger_report import generate_report
from displayResult import displayResult
from flask import Flask,jsonify
app=Flask(__name__)
@app.route('/get_report/<string:report_id>',methods=['GET'])
def get_report(report_id):
 finaldata=displayResult(report_id)
 return jsonify(finaldata)

@app.route('/trigger_report',methods=['GET'])    #trigger_report 
def trigger_report():
  return generate_report(4)
if __name__=="__main__":
  app.run(debug=True)