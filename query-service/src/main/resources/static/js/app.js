(function () {
    'use strict';

    angular.module('ledgerApp', [])

        // Read API is this server; the command API runs on port 8081 of the same host.
        .constant('API', {
            query: '/api/v1',
            command: window.location.protocol + '//' + window.location.hostname + ':8081/api/v1'
        })

        .factory('LedgerApi', ['$http', 'API', function ($http, API) {
            function data(r) { return r.data; }
            function idempotencyKey() {
                return (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now() + Math.random());
            }
            return {
                summary: function () { return $http.get(API.query + '/summary').then(data); },
                accounts: function () { return $http.get(API.query + '/accounts').then(data); },
                transactions: function (id) { return $http.get(API.query + '/accounts/' + id + '/transactions?limit=25').then(data); },
                alerts: function () { return $http.get(API.query + '/alerts?limit=20').then(data); },
                openAccount: function (body) {
                    return $http.post(API.command + '/accounts', body, {headers: {'Idempotency-Key': idempotencyKey()}}).then(data);
                },
                transfer: function (body) {
                    return $http.post(API.command + '/transfers', body, {headers: {'Idempotency-Key': idempotencyKey()}}).then(data);
                }
            };
        }])

        .controller('DashboardController', ['LedgerApi', '$interval', '$scope', '$q', function (api, $interval, $scope, $q) {
            var vm = this;
            vm.accounts = [];
            vm.alerts = [];
            vm.transactions = [];
            vm.summary = {};
            vm.open = {currency: 'USD'};
            vm.tx = {};

            vm.refresh = function () {
                var calls = [api.summary(), api.accounts(), api.alerts()];
                if (vm.selected) { calls.push(api.transactions(vm.selected.accountId)); }
                return $q.all(calls).then(function (r) {
                    vm.summary = r[0];
                    vm.accounts = r[1];
                    vm.alerts = r[2];
                    if (vm.selected) { vm.transactions = r[3]; }
                    vm.online = true;
                    vm.lastRefresh = new Date();
                }, function () { vm.online = false; });
            };

            vm.select = function (account) {
                vm.selected = account;
                api.transactions(account.accountId).then(function (t) { vm.transactions = t; });
            };

            vm.ownerOf = function (accountId) {
                var match = vm.accounts.filter(function (a) { return a.accountId === accountId; })[0];
                return match ? match.owner : accountId.substring(0, 8);
            };

            vm.ruleClass = function (rule) {
                return {LARGE_TRANSACTION: 'bg-danger', VELOCITY: 'bg-warning text-dark', VOLUME: 'bg-info text-dark'}[rule] || 'bg-secondary';
            };

            function reportError(e) {
                var body = e && e.data;
                vm.message = {ok: false, text: body && body.detail ? (body.code + ': ' + body.detail) : 'Command API unreachable'};
            }

            vm.openAccount = function () {
                api.openAccount(vm.open).then(function (r) {
                    vm.message = {ok: true, text: 'Account opened for ' + vm.open.owner + ' (' + r.id + ')'};
                    vm.open = {currency: vm.open.currency};
                }, reportError);
            };

            vm.transfer = function () {
                api.transfer(vm.tx).then(function (r) {
                    vm.message = {ok: true, text: 'Transfer accepted (' + r.id + ')'};
                    vm.tx = {};
                }, reportError);
            };

            vm.refresh();
            var timer = $interval(vm.refresh, 2000);
            $scope.$on('$destroy', function () { $interval.cancel(timer); });
        }]);
})();
