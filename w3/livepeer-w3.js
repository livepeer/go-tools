#!/usr/bin/env node

import fs from "fs";
import { CID } from "multiformats";
import { CarReader } from "@ipld/car";
import { ed25519 } from "@ucanto/principal";
import { Delegation } from "@ucanto/core";
import { AgentData } from "@web3-storage/access";
import { Client } from "@web3-storage/w3up-client";

async function getClient() {
    // create a client with UCAN private key passed in the env variable
    const principal = await ed25519.derive(
        Buffer.from(process.env.W3_PRINCIPAL_KEY, "base64")
    );
    const data = await AgentData.create({ principal });
    const client = new Client(data);

    // create a space with the delegation proof passsed in the env variable
    const blocks = [];
    const reader = await CarReader.fromBytes(
        Buffer.from(process.env.W3_DELEGATION_PROOF, "base64")
    );
    for await (const block of reader.blocks()) {
        blocks.push(block);
    }
    const proof = Delegation.importDAG(blocks);

    const space = await client.addSpace(proof);
    await client.setCurrentSpace(space.did());

    return client;
}

async function storeAdd(carPath) {
    const client = await getClient();

    let blob;
    try {
        const data = await fs.promises.readFile(carPath);
        blob = new Blob([data]);
    } catch (err) {
        console.log(err);
        process.exit(1);
    }

    const cid = await client.capability.store.add(blob);
    console.log(cid.toString());
}

async function uploadAdd(root, carCids) {
    const client = await getClient();

    let rootCID;
    try {
        rootCID = CID.parse(root);
    } catch (err) {
        console.error(`Error: failed to parse root CID: ${root}: ${err.message}`);
        process.exit(1);
    }

    const shards = [];
    for (const cid of carCids) {
        try {
            shards.push(CID.parse(cid));
        } catch (err) {
            console.error(`Error: failed to parse shard CID: ${cid}: ${err.message}`);
            process.exit(1);
        }
    }

    await client.capability.upload.add(rootCID, shards);
    console.log(root);
}

(async () => {
    try {
        const command = process.argv.slice(2, 5);
        const args = process.argv.slice(5);
        if (command.join(" ") === "can store add") {
            const carPath = args[0];
            await storeAdd(carPath);
        } else if (command.join(" ") === "can upload add") {
            const root = args[0];
            const carCids = args.slice(1);
            await uploadAdd(root, carCids);
        } else {
            console.log(`Invalid command: ${command.join(" ")}`);
            process.exit(1);
        }
    } catch (err) {
        console.log(err);
        process.exit(1);
    }
})();
