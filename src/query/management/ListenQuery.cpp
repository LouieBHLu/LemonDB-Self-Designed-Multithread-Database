//
// Created by lu on 21/10/19.
//
#include "ListenQuery.h"
#include "../../db/Database.h"

QueryResult::Ptr ListenQuery::execute() {
    return std::make_unique<ListenQueryResult>(std::move(fileName));
}

std::string ListenQuery::toString() {
    return "QUERY = LISTEN";
}



