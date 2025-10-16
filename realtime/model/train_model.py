# train_model.py (toy example)
import pandas as pd
from sklearn.linear_model import SGDClassifier
import joblib

# create toy dataset
df = pd.DataFrame({
    'temperature': [25,30,35,40,22,28,33,37],
    'humidity': [50,60,55,45,52,58,53,47],
    'label': [0,1,1,1,0,0,1,1]  # e.g., anomaly flag
})
X = df[['temperature','humidity']]
y = df['label']

clf = SGDClassifier(loss='log_loss', max_iter=1000, tol=1e-3)

clf.fit(X,y)

joblib.dump(clf, 'realtime/model/model.joblib')
print("model saved")
