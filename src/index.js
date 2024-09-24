/**
 * This type describes the options that your connector expects to recieve
 * This could include username + password, host + port, etc
 * @typedef {Object} ConnectorOptions
 * @property {string} SomeOption
 */

import { EvidenceType } from "@evidence-dev/db-commons";
import Stripe from 'stripe';

/**
 * @see https://docs.evidence.dev/plugins/create-source-plugin/#options-specification
 * @see https://github.com/evidence-dev/evidence/blob/main/packages/postgres/index.cjs#L316
 */
export const options = {
  apiKey: {
    title: "Stripe API Key",
    description: "Your Stripe secret API key",
    type: "string",
    required: true,
    secret: true
  }
};

function stringifyNestedObjects(obj) {
  return Object.fromEntries(
    Object.entries(obj).map(([key, value]) => {
      if (typeof value === 'object' && value !== null) {
        return [key, JSON.stringify(value)];
      }
      return [key, value];
    })
  );
}

/**
 * Implementing this function creates a "file-based" connector
 *
 * Each file in the source directory will be passed to this function, and it will return
 * either an array, or an async generator {@see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function*}
 * that contains the query results
 *
 * @see https://docs.evidence.dev/plugins/create-source-plugin/
 * @type {import("@evidence-dev/db-commons").GetRunner<ConnectorOptions>}
 */
export const getRunner = () => {
  return () => Promise.resolve();
};

// Uncomment to use the advanced source interface
// This uses the `yield` keyword, and returns the same type as getRunner, but with an added `name` and `content` field (content is used for caching)
// sourceFiles provides an easy way to read the source directory to check for / iterate through files
// /** @type {import("@evidence-dev/db-commons").ProcessSource<ConnectorOptions>} */
export async function* processSource(options, sourceFiles, utilFuncs) {
  const stripe = new Stripe(options.apiKey, {
    apiVersion: '2023-10-16'
  });

  const resources = [
    { name: 'customers', method: () => stripe.customers.list({ limit: 100 }) },
    { name: 'charges', method: () => stripe.charges.list({ limit: 100 }) },
    { name: 'invoices', method: () => stripe.invoices.list({ limit: 100 }) },
    { name: 'subscriptions', method: () => stripe.subscriptions.list({ limit: 100 }) },
    { name: 'products', method: () => stripe.products.list({ limit: 100 }) },
    { name: 'prices', method: () => stripe.prices.list({ limit: 100 }) },
    { name: 'paymentIntents', method: () => stripe.paymentIntents.list({ limit: 100 }) },
    { name: 'payouts', method: () => stripe.payouts.list({ limit: 100 }) },
    { name: 'refunds', method: () => stripe.refunds.list({ limit: 100 }) },
    { name: 'balanceTransactions', method: () => stripe.balanceTransactions.list({ limit: 100 }) },
    { name: 'events', method: () => stripe.events.list({ limit: 100 }) },
    { name: 'disputes', method: () => stripe.disputes.list({ limit: 100 }) },
  ];

  for (const resource of resources) {
    try {
      const data = await resource.method();
      const flattenedData = data.data.map(stringifyNestedObjects);
      
      if (flattenedData.length > 0) {
        const columnTypes = inferColumnTypes(flattenedData);

        yield {
          rows: flattenedData,
          columnTypes: columnTypes,
          expectedRowCount: flattenedData.length,
          name: resource.name,
          content: JSON.stringify({ resource: resource.name, apiKey: options.apiKey.substring(0, 5) + "..." }),
        };
      }
    } catch (error) {
      console.error(`Error fetching ${resource.name}:`, error);
    }
  }
}

function inferColumnTypes(rows) {
  if (rows.length === 0) return [];

  const sampleRow = rows[0];
  return Object.keys(sampleRow).map(key => {
    let evidenceType = EvidenceType.STRING;
    if (typeof sampleRow[key] === 'number') {
      evidenceType = EvidenceType.NUMBER;
    } else if (sampleRow[key] instanceof Date) {
      evidenceType = EvidenceType.DATE;
    }

    return {
      name: key,
      evidenceType: evidenceType,
      typeFidelity: "inferred",
    };
  });
}

/**
 * Implementing this function creates an "advanced" connector
 *
 *
 * @see https://docs.evidence.dev/plugins/create-source-plugin/
 * @type {import("@evidence-dev/db-commons").GetRunner<ConnectorOptions>}
 */

/** @type {import("@evidence-dev/db-commons").ConnectionTester<ConnectorOptions>} */
export const testConnection = async (opts) => {
  try {
    const stripe = new Stripe(opts.apiKey);
    await stripe.customers.list({ limit: 1 });
    return true;
  } catch (error) {
    console.error("Stripe connection test failed:", error);
    return false;
  }
};
