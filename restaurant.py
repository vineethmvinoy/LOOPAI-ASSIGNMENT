from trigger_report import generate_report
from displayResult import displayResult
from flask import Flask
app=Flask(__name__)
@app.route('/get_report/<string:report_id>',methods=['GET'])
def get_report(report_id):
 finaldata=displayResult(report_id)
 return finaldata

@app.route('/trigger_report',methods=['GET'])
def homepage():
  return generate_report(30)
if __name__=="__main__":
  app.run(debug=True)