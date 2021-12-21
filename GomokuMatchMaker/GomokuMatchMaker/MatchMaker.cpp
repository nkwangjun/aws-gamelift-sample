/*
* Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*  http://aws.amazon.com/apache2.0
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

#include "stdafx.h"
#include "SharedStruct.h"
#include "Log.h"
#include "MatchMaker.h"
#include "PlayerSession.h"

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/gamelift/model/SearchGameSessionsRequest.h>
#include <aws/gamelift/model/CreateGameSessionRequest.h>
#include <aws/gamelift/model/CreatePlayerSessionsRequest.h>

#include <aws/core/utils/memory/stl/AWSSet.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/DynamoDBErrors.h>
#include <aws/core/utils/UnreferencedParam.h>
#include <aws/core/utils/memory/stl/AWSSet.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#undef IN
#include <aws/dynamodb/model/PutItemRequest.h>
//#define IN
#include <aws/dynamodb/model/GetItemRequest.h>


std::unique_ptr<MatchMaker> GMatchMaker;


using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::DynamoDB;
using namespace Aws::DynamoDB::Model;


MatchMaker::MatchMaker(const std::string& alias, const std::string& playerTable) : mAliasId(alias), mPlayerTableName(playerTable)
{
}

void MatchMaker::SetUpAwsClient(const std::string& region)
{
	Aws::Client::ClientConfiguration config;
	config.scheme = Aws::Http::Scheme::HTTPS;
	config.connectTimeoutMs = 10000;
	config.requestTimeoutMs = 10000;

	config.region = region;
	
	mDDBClient = Aws::MakeShared<Aws::DynamoDB::DynamoDBClient>("GameLiftMatchMaker", config);

	/// In case of GameLift Local
	if (mAliasId == "TEST_LOCAL")
	{
		config.scheme = Aws::Http::Scheme::HTTP;
		config.endpointOverride = "127.0.0.1:9080";
	}

	config.scheme = Aws::Http::Scheme::HTTP;
	config.endpointOverride = "127.0.0.1:9080";

	mGLClient = Aws::MakeShared<Aws::GameLift::GameLiftClient>("GameLiftMatchMaker", config);

}


bool MatchMaker::RequestMatch(std::shared_ptr<PlayerSession> psess)
{
	FastSpinlockGuard lock(mMatchLock);

	/// check if already exists (idempotention check)
	auto it = mMatchMap.find(psess->GetPlayerName());
	if (it == mMatchMap.end())
	{
		mMatchMap[psess->GetPlayerName()] = psess;
		return true;
	}

	return false;
}

void MatchMaker::DoMatchMaking()
{

	std::thread matchThread([=] 
	{
		while (true)
		{
			std::shared_ptr<PlayerSession> player1;
			std::shared_ptr<PlayerSession> player2;
			
			mMatchLock.EnterReadLock();
			if (mMatchMap.size() >= MAX_PLAYER_PER_GAME)
			{
				auto it = mMatchMap.begin();
				player1 = it->second;
				++it;
				player2 = it->second;
			}
			mMatchLock.LeaveReadLock();

			/// match success!
			if (player1 && player2)
			{
				if (!player1->IsConnected())
				{
					mMatchLock.EnterWriteLock();
					mMatchMap.erase(player1->GetPlayerName());
					mMatchLock.LeaveWriteLock();
					continue;
				}

				if (!player2->IsConnected())
				{
					mMatchLock.EnterWriteLock();
					mMatchMap.erase(player2->GetPlayerName());
					mMatchLock.LeaveWriteLock();
					continue;
				}

				/// when no available game sessions...
				Aws::GameLift::Model::CreateGameSessionRequest req;
				req.SetFleetId("Fleet-123");
				req.SetMaximumPlayerSessionCount(MAX_PLAYER_PER_GAME);

				auto outcome = mGLClient->CreateGameSession(req);
				if (outcome.IsSuccess())
				{
					auto gs = outcome.GetResult().GetGameSession();
					auto port = gs.GetPort();
					auto ipAddress = gs.GetIpAddress();
					auto gameSessionId = gs.GetGameSessionId();

					std::cout << "CreatePlayerSessions on Created Game Session\n";
					CreatePlayerSessions(player1, player2, ipAddress, gameSessionId, port);

				}
				else
				{
					GConsoleLog->PrintOut(true, "CreateGameSession error: %s\n", outcome.GetError().GetExceptionName().c_str());
				}

				/// remove players from the match queue
				mMatchLock.EnterWriteLock();
				mMatchMap.erase(player1->GetPlayerName());
				mMatchMap.erase(player2->GetPlayerName());
				mMatchLock.LeaveWriteLock();
			}
				
			
			Sleep(100);
		}
	});

	matchThread.detach();
}

void MatchMaker::CreatePlayerSessions(std::shared_ptr<PlayerSession> player1, std::shared_ptr<PlayerSession> player2, const std::string& ipAddress, const std::string& gsId, int port)
{
	Aws::GameLift::Model::CreatePlayerSessionsRequest req;
	req.SetGameSessionId(gsId);
	std::vector<std::string> playerNames;
	playerNames.push_back(player1->GetPlayerName());
	playerNames.push_back(player2->GetPlayerName());
	req.SetPlayerIds(playerNames);

	std::map<std::string, std::string> scoreMap;
	scoreMap[player1->GetPlayerName()] = std::to_string(player1->GetPlayerScore());
	scoreMap[player2->GetPlayerName()] = std::to_string(player2->GetPlayerScore());
	req.SetPlayerDataMap(scoreMap);

	auto result = mGLClient->CreatePlayerSessions(req);
	auto player1SessionId = player1->GetPlayerName();
	auto player2SessionId = player2->GetPlayerName();

	player1->SendMatchInfo(port, ipAddress, player1SessionId);
	player2->SendMatchInfo(port, ipAddress, player2SessionId);

}


bool MatchMaker::PlayerLogin(std::shared_ptr<PlayerSession> psess, const std::string& playerName, const std::string& playerPass)
{
	if (playerName.size() < 3 || playerPass.size() < 3)
		return false;

	/// password hashing first
	std::size_t passwd_hash = std::hash<std::string>{}(playerPass);


	int score = 98;
	psess->SetPlayerInfo(playerName, score);

	return true;
}

