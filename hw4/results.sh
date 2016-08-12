cp ~/Documents/workspace/QryEval/HW4-queries_exp.teIn .
rm exp_result
perl fetchUrl.pl >> exp_result
cat exp_result | grep all | grep -e P10  -e P20 -e P30 -e map
python request.py sdm exp_result