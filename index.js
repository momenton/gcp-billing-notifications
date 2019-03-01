const slack = require('slack');
const {BigQuery} = require('@google-cloud/bigquery');

const CHANNEL = 'gcp-notifications';
const DATASET = 'billing'
const TABLE = 'budget';
const PROJECT = process.env.GCP_PROJECT;
const DATASET_LOCATION = 'australia-southeast1';
const bigquery = new BigQuery();
const BOT_ACCESS_TOKEN = process.env.BOTTOKEN;

exports.notifySlack = async (data, context) => {
  
	const pubsubMessage = data;
    const pubsubData = JSON.parse(Buffer.from(pubsubMessage.data, 'base64').toString());
    const formatter = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', minimumFractionDigits: 2})
    const budgetId = pubsubMessage.attributes.budgetId;
    const costAmount = formatter.format(pubsubData.costAmount);
    const budgetAmount = formatter.format(pubsubData.budgetAmount);
    const budgetName = pubsubData.budgetDisplayName;
    const threshold = (pubsubData.alertThresholdExceeded*100).toFixed(0);
    const createdAt = new Date().toISOString();

    if (!isFinite(threshold)){
        threshold = 0;
    }
    //save data 
    const rows = [{createdAt: createdAt, 
                    costAmount: pubsubData.costAmount, 
                    budgetAmount:pubsubData.budgetAmount, 
                    budgetId: budgetId,
                    budgetName: budgetName,
                    threshold: threshold}]

    
    await bigquery.dataset(DATASET).table(TABLE).insert(rows);
    

    const query = `SELECT count(*) cnt
                    FROM \`${PROJECT}.${DATASET}.${TABLE}\`
                    WHERE createdAt >  TIMESTAMP( DATE(EXTRACT(YEAR FROM CURRENT_DATE()) , EXTRACT(MONTH FROM CURRENT_DATE()), 1)) 
                    AND Threshold = ${threshold} and BudgetId = '${budgetId}'
                    `;

    const options = {
        query: query,
        location: DATASET_LOCATION,
    };

  
    const [results]  = await bigquery.query(options);
    
    if (results.length > 0 && results[0].cnt > 1 ){
        return;
    }
    
    const emoticon = threshold >= 90 ? ':fire:' : ''; 

    notification = `${emoticon} ${budgetName} \nThis is an automated notification to inform you that your billing account has exceeded ${threshold}% of the monthly budget of ${budgetAmount}.\nThe billing account has accrued ${costAmount} in costs so far for the month.`

  
    const res = await slack.chat.postMessage({
        token: BOT_ACCESS_TOKEN,
        channel: CHANNEL,
        text: notification
    });
 
   
};