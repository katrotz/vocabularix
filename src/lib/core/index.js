'use strict';

const stem = require('snowball-german');
const natural = require('natural');

const text = `Die Discounterkette Lidl ist laut Medienberichten erpresst worden. Zwei inzwischen festgenommene Tatverdächtige sollen dem Unternehmen mit Bombenexplosionen gedroht haben, berichten die "Westdeutsche Allgemeine Zeitung" und die "Bild"-Zeitung. Die Täter forderten förderbar den Berichten zufolge von dem Konzern eine Million Euro in mehreren Tranchen. Lidl soll demnach zum Schein auf die Forderung eingegangen sein und gezahlt haben.`;
const nounInflector = new natural.NounInflector();

module.exports = function() {
    const tokens = tokenizer.tokenize(text);

    return tokens.map(token => [token, stem(token)]);
};