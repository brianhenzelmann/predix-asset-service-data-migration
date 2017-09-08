'use strict';

const Promise = require('bluebird');
const request = Promise.promisifyAll(require('request'));
const _ = require('lodash');
const chalk = require('chalk');
const localConfig = require('./asset-config.json');

const queryLimit = 1000;

let originalToken = '';
let destinationToken = '';

/**
 * Send HTTPS request to retrieve client token
 *
 * @param {String} uaaUrl
 * @param {String} credentials
 */
const requestToken = function requestToken(uaaUrl, credentials) {
  return new Promise(function(resolve, reject) {
    request({
      url: uaaUrl + '/oauth/token',
      headers: {
        'Authorization': `Basic ${credentials}`,
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      method: 'POST',
      body: 'grant_type=client_credentials&response_type=token'
    }, function (error, response, body) {
      if (!error && response && response.statusCode === 200) {
        resolve(JSON.parse(body));
      } else {
        reject();
      }
    }, function () {
      reject();
    });
  });
};

/**
 * Get or post data for an asset service instance
 *
 * @param {String} url
 * @param {String} zoneId
 * @param {String} token
 * @param {String} [method]
 * @param {Object} [body]
 */
const requestAssetData = function requestAssetData (url, zoneId, token, method, body) {
  return new Promise((resolve, reject) => {
    try {
      request({
        url: url,
        headers: {
          'Predix-Zone-Id': zoneId,
          'Authorization': token,
          'x-force-write': method === 'POST' ? true : false
        },
        method: method ? method : 'GET',
        json: body ? true : false,
        body: body ? body : null
      }, function (error, response, body) {
        if (error || (response && response.statusCode.toString().indexOf('2') !== 0)) {
          reject();
          return;
        } else {
          resolve({
            body: body ? JSON.parse(body) : null,
            headers: response && response.headers ? response.headers : {}
          });
          return;
        }
      });
    } catch (e) {
      reject();
    }
  });
};

/**
 * Get all domain object instances
 * @param {String} url
 * @param {String} zoneId
 * @param {String} token
 * @param {Array} instances
 * @param {String} target
 */
const getDomainObjectInstances = function getDomainObjectInstances (url, zoneId, token, instances, target) {
  return new Promise((resolve, reject) => {
    requestAssetData(url, zoneId, token).then((response) => {
      instances = instances.concat(response.body);
      console.log(`Loaded ${chalk.cyan(instances.length)} of ${chalk.cyan(target)} domain object instances`);
      if (response.headers && response.headers.link) {
        const nextUrl = response.headers.link.substring(response.headers.link.indexOf('<') + 1, response.headers.link.indexOf('>') - 1);
        getDomainObjectInstances (nextUrl, zoneId, token, instances, target).then((fullList) => {
          resolve(fullList);
        });
      } else {
        resolve(instances);
      }
    }, (error) => {
      reject();
    });
  });
};

/**
 * Handle the migration of the Predix Asset service
 */
const migrate = function migrate () {
  // List of token promises
  const uaaTokenPromises = [];

  console.log(chalk.bold(`Retrieving token for origin ${chalk.cyan(localConfig.originalAssetZoneId)}\n`));
  const originTokenRequest = requestToken(localConfig.originalUaaUrl, localConfig.originalUaaCredentials);
  uaaTokenPromises.push(originTokenRequest);
  originTokenRequest.then((tokenResponse) => {
    originalToken = `Bearer ${tokenResponse.access_token}`;
  }, () => {
    console.error(chalk.red('Error loading the origin token.'));
  });

  console.log(chalk.bold(`Retrieving token for destination ${chalk.cyan(localConfig.destinationAssetZoneId)}\n`));
  const destinationTokenRequest = requestToken(localConfig.destinationUaaUrl, localConfig.destinationUaaCredentials);
  uaaTokenPromises.push(destinationTokenRequest);
  destinationTokenRequest.then((tokenResponse) => {
    destinationToken = `Bearer ${tokenResponse.access_token}`;
  }, () => {
    console.error(chalk.red('Error loading the destination token.'));
  });

  Promise.all(uaaTokenPromises).then(() => {
    console.log(chalk.bold.green(`OK\n`));
    console.log(chalk.bold(`Retrieving domain object instances for instance ${chalk.cyan(localConfig.originalAssetZoneId)}`));
    requestAssetData(localConfig.originalAssetUrl, localConfig.originalAssetZoneId, originalToken).then((response) => {
      const domainObjects = response.body;
      console.log(`Found ${chalk.cyan(domainObjects.length)} domain objects with a total of ${chalk.cyan(_.sumBy(domainObjects, 'count'))} domain object instances\n`);

      // Loop through each domain object and load its domain object instances
      _.each(domainObjects, (domainObject) => {
        console.log(chalk.bold(`Loading data for the ${chalk.cyan(domainObject.collection)} domain object`));
        const originDomainObjectUrl = `${localConfig.originalAssetUrl}/${domainObject.collection}?pageSize=${queryLimit}`;

        getDomainObjectInstances(originDomainObjectUrl, localConfig.originalAssetZoneId, originalToken, [], domainObject.count).then((instances) => {
          console.log(`\nFinished loading ${chalk.cyan(instances.length)} domain object instances\n`);
          console.log(chalk.bold(`Posting ${chalk.cyan(instances.length)} domain object instances to ${chalk.cyan(localConfig.destinationAssetZoneId)}`));

          const promiseList = [];
          _.each(_.chunk(instances, queryLimit), (chunk) => {
            console.log(`Posting ${chalk.cyan(chunk.length)} to the destination asset service instance`);
            const destinationDomainObjectUrl = `${localConfig.destinationAssetUrl}/${domainObject.collection}`;
            promiseList.push(requestAssetData(destinationDomainObjectUrl, localConfig.destinationAssetZoneId, destinationToken, 'POST', chunk.map((item) => {
              item.migrationDate = new Date();
              return item;
            })));
          });
          Promise.all(promiseList).then(() => {
            console.log(`\nFinished posting ${chalk.cyan(instances.length)} domain object instances\n`);
          }, () => {
            console.error(chalk.red(`There was an error posting the domain object instances`));
          });
        });
      });
    }, (error) => {
      console.error(chalk.red('There was an error listing asset domain objects'));
    });
  }, () => {
    console.error(chalk.bold('\nPlease check your UAA credentials'));
  });
};

migrate();
