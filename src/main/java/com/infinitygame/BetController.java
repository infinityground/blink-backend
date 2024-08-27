package com.infinitygame;

import cn.hutool.json.JSONUtil;
import com.infinitygame.biz.service.BetService;
import com.infinitygame.infrastructure.model.Result;
import com.infinitygame.model.bet.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;

@Api(value = "/bet", tags = "bet")
@RestController
@AllArgsConstructor
@RequestMapping("/bet")
@Slf4j
public class BetController {

    @Autowired
    private BetService betService;

    @ApiOperation(value = "获取图片")
    @PostMapping("/getImg")
    public Result<GetImgResponse> getBetImg() {
        return Result.ok(betService.getBetImg());
    }

    @ApiOperation(value = "通过ID获取图片")
    @PostMapping("/getImgById")
    public Result<GetImgResponse> getBetImgById(@RequestBody @Valid GetImgRequest getImgRequest) {
        return Result.ok(betService.getBetImgById(getImgRequest.getImgId()));
    }

    @ApiOperation(value = "推送链上交易")
    @PostMapping("/pushData")
    public Result<Void> pushData(@RequestBody List<PushDataRequest> request) {
        log.info("all pushData, request: {}", JSONUtil.toJsonStr(request));
        for (PushDataRequest pushDataRequest : request) {
            betService.receivePushData(pushDataRequest);
        }
        return Result.ok();
    }

    @ApiOperation(value = "是否可以claim")
    @PostMapping("/canClaim")
    public Result<CanClaimResponse> canClaim(@RequestBody @Valid ClaimRequest request) {
        return Result.ok(betService.canClaim(request));
    }

    @ApiOperation(value = "claim")
    @PostMapping("/claim")
    public Result<Void> claim(@RequestBody ClaimRequest request) {
        betService.claim(request);
        return Result.ok();
    }

}
