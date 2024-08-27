package com.infinitygame;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.infinitygame.biz.service.solana.SolanaService;
import com.infinitygame.infrastructure.dal.TransferDao;
import com.infinitygame.infrastructure.domain.BetRecord;
import com.infinitygame.infrastructure.domain.ImgConfig;
import com.infinitygame.infrastructure.domain.TransferRecord;
import com.infinitygame.infrastructure.enums.TokenEnum;
import com.infinitygame.infrastructure.enums.TransferSourceTypeEnum;
import com.infinitygame.infrastructure.enums.TransferStatusEnum;
import com.infinitygame.model.bet.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;
import org.p2p.solanaj.rpc.RpcException;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.infinitygame.biz.service.solana.SolanaService.LAMPORTS_EXCHANGE_RATE;

@Service
@Slf4j
public class BetService {

    @Autowired
    private TransferDao transferDao;

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private SolanaService solanaService;

    @Value("${bet.solana.payer.address}")
    private String betPayerSolanaAddress;

    @Value("${bet.solana.privateKey}")
    private String betPrivateKey;

    public GetImgResponse getBetImg() {
        // 获取配置中的所有图片id
        List<Long> imgConfigAllId = transferDao.listImgConfigAllId();
        int index = RandomUtil.randomInt(imgConfigAllId.size());
        GetImgResponse betImgById = getBetImgById(imgConfigAllId.get(index));
        betImgById.setAnswerList(null);
        return betImgById;
    }

    public GetImgResponse getBetImgById(Long imgConfigAllId) {
        ImgConfig imgConfig = transferDao.getImgConfig(imgConfigAllId);
        if (imgConfig == null) {
            // 返回兜底图片
            imgConfig = transferDao.getImgConfig(1L);
        }
        GetImgResponse getImgResponse = new GetImgResponse();
        getImgResponse.setImgId(imgConfig.getId());
        getImgResponse.setImgUrl(imgConfig.getImgUrl());
        getImgResponse.setAnswerList(parseAnswerList(imgConfig));
        return getImgResponse;
    }


    public void receivePushData(PushDataRequest pushDataRequest) {
        log.info("receivePushData, request: {}", JSONUtil.toJsonStr(pushDataRequest));
        CreateTxRequest createTxRequest = new CreateTxRequest();
        JSONObject meta = pushDataRequest.getMeta();
        JSONArray logMessages = (JSONArray) meta.get("logMessages");
        for (Object logMessage : logMessages) {
            String logMessageStr = String.valueOf(logMessage);
            if (logMessageStr.startsWith("Program log: Memo ")) {
                // 从冒号分割
                String[] parts = logMessageStr.split(":", 3);
                //  \"{\\\"answer\\\":\\\"test\\\",\\\"bet\\\":0.1,\\\"imgId\\\":1}\"
                String request = parts[parts.length - 1].trim();
                String result = request.replaceAll("^\"|\"$", "");
                JSONObject jsonObject = JSONUtil.parseObj(StringEscapeUtils.unescapeJava(result));
                createTxRequest.setUserAnswer(jsonObject.getStr("answer"));
                createTxRequest.setAmount(new BigDecimal(jsonObject.getStr("bet")));
                createTxRequest.setImgId(jsonObject.getLong("imgId"));
            }
        }
        if (createTxRequest.getAmount() == null || StringUtils.isEmpty(createTxRequest.getUserAnswer()) || createTxRequest.getImgId() == null) {
            log.error("data invalid! no amount or no userAnswer");
            return;
        }
        JSONObject transaction = pushDataRequest.getTransaction();
        JSONObject message = (JSONObject) transaction.get("message");
        JSONArray accountKeys = (JSONArray) message.get("accountKeys");
        String walletAddress = accountKeys.getStr(0);
        String receiveAddress = accountKeys.getStr(1);
        log.info("parsed walletAddress: {}, receiveAddress: {}", walletAddress, receiveAddress);
        if (StringUtils.isEmpty(walletAddress) || StringUtils.isEmpty(receiveAddress)) {
            log.error("data invalid! no amount or no userAnswer");
            return;
        }
        if (!Objects.equals(receiveAddress, betPayerSolanaAddress)) {
            log.error("receiveAddress invalid");
            return;
        }
        createTxRequest.setWalletAddress(walletAddress);
        // 记录已上链的交易
        createTx(createTxRequest);
    }

    private void createTransfer(ClaimRequest claimRequest, List<TransferDto> transferList) {
        if (CollectionUtils.isEmpty(transferList)) {
            return;
        }
        for (TransferDto transfer : transferList) {
            TransferRecord transferRecord = new TransferRecord();
            transferRecord.setWalletAddress(claimRequest.getWalletAddress());
            transferRecord.setFromAddress(betPayerSolanaAddress);
            transferRecord.setToAddress(claimRequest.getWalletAddress());
            transferRecord.setAmount(transfer.getAmount());
            transferRecord.setLamportsAmount(transfer.getLamportsAmount());
            transferRecord.setImgId(transfer.getImgId());
            transferRecord.setSign(transfer.getSign());
            transferRecord.setSourceType(TransferSourceTypeEnum.BET.name());
            transferRecord.setSourceId(String.valueOf(transfer.getBetRecordId()));
            transferRecord.setToken(TokenEnum.SOL.getName());
            if (BooleanUtil.isTrue(transfer.getTxResult())) {
                transferRecord.setStatus(TransferStatusEnum.SUCCESS.getCode());
            } else {
                transferRecord.setStatus(TransferStatusEnum.FAIL.getCode());
            }
            transferDao.createTransferRecord(transferRecord);
        }
    }


    public void createTx(CreateTxRequest request) {
        ImgConfig imgConfig = transferDao.getImgConfig(request.getImgId());
        if (imgConfig == null) {
            // 返回兜底图片
            imgConfig = transferDao.getImgConfig(1L);
        }
        BetRecord betRecord = new BetRecord();
        betRecord.setAmount(request.getAmount());
        betRecord.setImgId(request.getImgId());
        betRecord.setUserAnswer(request.getUserAnswer());
        betRecord.setWalletAddress(request.getWalletAddress());

        betRecord.setImgUrl(imgConfig.getImgUrl());
        List<String> answerList = parseAnswerList(imgConfig);
        // answer转小写
        List<String> lowCaseAnswerList = answerList.stream().map(String::toLowerCase).collect(Collectors.toList());
        betRecord.setAnswersSnapshot(lowCaseAnswerList.toString());
        // 用户的答案转小写
        boolean match = lowCaseAnswerList.stream().anyMatch(a -> Objects.equals(a, request.getUserAnswer().toLowerCase()));
        betRecord.setMatchResult(String.valueOf(match));
        transferDao.createBetRecord(betRecord);
        log.info("createTx success, request: {}, betRecord: {}", JSONUtil.toJsonStr(request), JSONUtil.toJsonStr(betRecord));
    }

    private List<String> parseAnswerList(ImgConfig imgConfig) {
        List<String> answerList = new ArrayList<>();
        try {
            answerList = JSONUtil.toList(imgConfig.getAnswers(), String.class);
        } catch (Exception e) {
            // 报错则按字符串匹配
            answerList.add(imgConfig.getAnswers());
            log.error("json error! answerList: {}", answerList);
        }
        return answerList;
    }

    public CanClaimResponse canClaim(ClaimRequest claimRequest) {
        CanClaimResponse canClaimResponse = new CanClaimResponse();
        canClaimResponse.setCanClaim(false);
        List<Long> allMatchBetRecordImgIdList = transferDao.getAllMatchBetRecordImgIdList(claimRequest.getWalletAddress(), String.valueOf(true));
        if (CollectionUtils.isEmpty(allMatchBetRecordImgIdList)) {
            log.info("not match!, claimRequest : {}, allMatchBetRecordImgIdList: {}", claimRequest, allMatchBetRecordImgIdList);
            canClaimResponse.setMatchResult(false);
            return canClaimResponse;
        }
        canClaimResponse.setMatchResult(true);
        boolean canClaim = false;
        boolean claimed = true;
        // 查数据库之前有没有转过账成功
        List<TransferRecord> successTransferRecords = transferDao.getTransferRecord(allMatchBetRecordImgIdList, claimRequest.getWalletAddress(), TransferStatusEnum.SUCCESS.getCode(), false);
        List<Long> successTransferRecordImgIdList = successTransferRecords.stream().map(TransferRecord::getImgId).distinct().collect(Collectors.toList());
        log.info("canClaim claimRequest: {} , allMatchBetRecordImgIdList size: {}, successTransferRecordImgIdList size: {}", claimRequest, allMatchBetRecordImgIdList.size(), successTransferRecordImgIdList.size());
        if (successTransferRecordImgIdList.size() < allMatchBetRecordImgIdList.size()) {
            claimed = false;
            canClaim = true;
        }
        canClaimResponse.setClaimed(claimed);
        canClaimResponse.setCanClaim(canClaim);
        return canClaimResponse;
    }

    public void claim(ClaimRequest claimRequest) {
        log.info("claim start! request: {}", claimRequest);
        List<BetRecord> matchBetRecordList = transferDao.getAllMatchBetRecord(claimRequest.getWalletAddress(), String.valueOf(true));
        if (CollectionUtils.isEmpty(matchBetRecordList)) {
            log.info("not match!, matchBetRecordList: {}", matchBetRecordList);
            return;
        }
        // 查数据库之前有没有转过账成功
        List<Long> allMatchBetRecordImgIdList = matchBetRecordList.stream().map(BetRecord::getImgId).distinct().collect(Collectors.toList());
        List<TransferRecord> successTransferRecords = transferDao.getTransferRecord(allMatchBetRecordImgIdList, claimRequest.getWalletAddress(), TransferStatusEnum.SUCCESS.getCode(), false);
        List<Long> successTransferRecordImgIdList = successTransferRecords.stream().map(TransferRecord::getImgId).distinct().collect(Collectors.toList());
        log.info("claim claimRequest: {} , allMatchBetRecordImgIdList size: {}, successTransferRecordImgIdList size: {}", claimRequest, allMatchBetRecordImgIdList.size(), successTransferRecordImgIdList.size());
        if (successTransferRecordImgIdList.size() >= allMatchBetRecordImgIdList.size()) {
            log.info("already claimed!");
            return;
        }
        // 过滤掉已经转过的
        Set<Long> allMatchBetRecordImgIdSet = new HashSet<>(allMatchBetRecordImgIdList);
        Set<Long> successTransferRecordImgIdSet = new HashSet<>(successTransferRecordImgIdList);
        allMatchBetRecordImgIdSet.removeAll(successTransferRecordImgIdSet);
        Map<Long, List<BetRecord>> imgIdBetRecordListMap = matchBetRecordList.stream().filter(record -> allMatchBetRecordImgIdSet.contains(record.getImgId())).collect(Collectors.groupingBy(BetRecord::getImgId));
        List<BetRecord> filterMatchBetRecordList = new ArrayList<>();
        imgIdBetRecordListMap.forEach((imgId, betRecordList) -> {
            if (CollectionUtils.isEmpty(betRecordList)) {
                return;
            }
            if (betRecordList.size() == 1) {
                filterMatchBetRecordList.add(betRecordList.get(0));
                return;
            }
            filterMatchBetRecordList.stream().max(Comparator.comparing(BetRecord::getCreateTime)).ifPresent(filterMatchBetRecordList::add);
        });

        List<TransferDto> transferDtoList = transfer(filterMatchBetRecordList, claimRequest.getWalletAddress());
        createTransfer(claimRequest, transferDtoList);
        log.info("claim success! request: {}, transferDtoList size: {}, transferDtoList: {}", claimRequest, transferDtoList.size(), JSONUtil.toJsonStr(transferDtoList));
    }

    public List<TransferDto> transfer(List<BetRecord> betRecordList, String walletAddress) {
        log.info("transfer start, betRecordList size: {}, betRecordListStr: {}", betRecordList.size(), JSONUtil.toJsonStr(betRecordList));
        List<TransferDto> result = new ArrayList<>();
        // 加锁
        String betTransferLock = "transfer:%s";
        String lockKey = String.format(betTransferLock, walletAddress);
        RLock lock = redissonClient.getLock(lockKey);
        long totalLamports = 0;
        try {
            lock.tryLock(2, 60 * 60, TimeUnit.SECONDS);
            if (!lock.isLocked()) {
                log.warn("transfer lock error! lockKey: {}", lockKey);
                return result;
            }
            // 转账 并记录
            // 判断池子是否有余额
            Long poolBalance = solanaService.getBalance(betPayerSolanaAddress);
            log.info("poolBalance: {}", poolBalance);
            for (BetRecord betRecord : betRecordList) {
                // 是否匹配成功
                String betRecordStr = JSONUtil.toJsonStr(betRecord);
                if (!Objects.equals(betRecord.getMatchResult(), String.valueOf(true))) {
                    log.info("not match!, betRecord: {}", betRecordStr);
                    continue;
                }
                // 获取到betRecord
                if (betRecord.getAmount().compareTo(new BigDecimal(0)) <= 0) {
                    log.info("amount is zero!, betRecord: {}", betRecordStr);
                    continue;
                }
                // 查数据库之前有没有转过账成功
                List<TransferRecord> successTransferRecords = transferDao.getTransferRecord(Lists.newArrayList(betRecord.getImgId()), betRecord.getWalletAddress(), TransferStatusEnum.SUCCESS.getCode(), true);
                if (!CollectionUtils.isEmpty(successTransferRecords)) {
                    log.info("transfer already success!, betRecord: {}", betRecordStr);
                    continue;
                }
                BigDecimal betSol = betRecord.getAmount();
                long betLamports = NumberUtil.div(betSol, LAMPORTS_EXCHANGE_RATE).longValue();
                long maxReward = NumberUtil.mul(new BigDecimal(betLamports), new BigDecimal("0.2")).longValue();
                long reward = NumberUtil.mul(new BigDecimal(poolBalance), new BigDecimal("0.01")).longValue();
                if (reward > maxReward) {
                    reward = maxReward;
                }
                long amount = betLamports + reward;
                totalLamports += amount;

                TransferDto transferDto = new TransferDto();
                transferDto.setLamportsAmount(amount);
                transferDto.setAmount(NumberUtil.mul(new BigDecimal(amount), LAMPORTS_EXCHANGE_RATE).setScale(4, RoundingMode.HALF_UP));
                transferDto.setImgId(betRecord.getImgId());
                transferDto.setBetRecordId(betRecord.getId());
                result.add(transferDto);
            }


            // 池子小于下注的钱，返回false
            if (poolBalance == null || poolBalance <= totalLamports) {
                log.info("transfer poolBalance is not enough!");
                for (TransferDto transferDto : result) {
                    transferDto.setTxResult(false);
                }
                return result;
            }

            String sign = solanaService.sendTx(betPayerSolanaAddress, walletAddress, totalLamports, betPrivateKey);
            log.info("sendTx success! signature: {}, amount: {}", sign, totalLamports);
            for (TransferDto transferDto : result) {
                transferDto.setTxResult(true);
                transferDto.setSign(sign);
            }
            return result;

        } catch (InterruptedException | RpcException e) {
            log.error("transfer failed!", e);
            return result;
        } finally {
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }

    }


}
