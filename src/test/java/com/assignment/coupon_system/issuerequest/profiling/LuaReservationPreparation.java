package com.assignment.coupon_system.issuerequest.profiling;

import com.assignment.coupon_system.coupon.redis.*;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import java.time.Instant;
import java.util.*;

/** Experimental single-Redis/single-relay adapter; never component-scanned into production. */
public class LuaReservationPreparation {
 private final StringRedisTemplate redis;
 public LuaReservationPreparation(StringRedisTemplate redis){this.redis=redis;}
 private static final DefaultRedisScript<List> SCRIPT=new DefaultRedisScript<>("""
   -- Validate every read and value BEFORE the sole mutating command. Lua errors do not roll back.
   for i=1,2 do
     local t=redis.call('TYPE',KEYS[i]).ok
     if t~='none' and t~='list' then return redis.error_reply('Invalid queue type') end
   end
   local id=redis.call('LINDEX',KEYS[2],0)
   local existing=id~=false
   if not existing then id=redis.call('LINDEX',KEYS[1],0) end
   if not id then return {} end
   local key=ARGV[1]..id
   if redis.call('TYPE',key).ok~='hash' then return redis.error_reply('Missing or invalid reservation hash') end
   local v=redis.call('HMGET',key,'requestId','couponId','userId','reservedAtMillis','policyVersion')
   for i=1,5 do if not v[i] then return redis.error_reply('Missing reservation field') end end
   if v[1]~=id then return redis.error_reply('Reservation requestId mismatch') end
   -- Validate Java signed long without converting to Lua doubles (which lose precision above 2^53).
   for i=2,5 do
     local s=v[i]
     local negative=string.sub(s,1,1)=='-'
     if negative or string.sub(s,1,1)=='+' then s=string.sub(s,2) end
     if not string.match(s,'^%d+$') then return redis.error_reply('Invalid integer') end
     s=string.gsub(s,'^0+','')
     local limit=negative and '9223372036854775808' or '9223372036854775807'
     if #s>19 or (#s==19 and s>limit) then return redis.error_reply('Integer overflow') end
   end
   if not existing then redis.call('LMOVE',KEYS[1],KEYS[2],'LEFT','RIGHT') end
   return v
   """,List.class);
 public CouponReservation prepare(){
  List<?> v=redis.execute(SCRIPT,List.of(CouponRedisKey.pendingReservations(),CouponRedisKey.processingReservations()),"coupon:reservation:");
  if(v==null)throw new IllegalStateException("Missing Lua result");
  if(v.isEmpty())return null;
  if(v.size()!=5)throw new IllegalStateException("Invalid Lua result");
  return new CouponReservation((String)v.get(0),Long.parseLong((String)v.get(1)),Long.parseLong((String)v.get(2)),Instant.ofEpochMilli(Long.parseLong((String)v.get(3))),Long.parseLong((String)v.get(4)));
 }
 /** Keep the real production relayOne/send/completeDelivery; adapt only its preparation calls. */
 public CouponIssueRedisService wrap(CouponIssueRedisService target){
  ThreadLocal<CouponReservation> prepared=new ThreadLocal<>();
  ProxyFactory proxy=new ProxyFactory(target);proxy.setProxyTargetClass(true);
  proxy.addAdvice((MethodInterceptor)i->{
   if(i.getMethod().getName().equals("peekProcessingReservation")){
    prepared.remove();CouponReservation r=prepare();
    if(r!=null)prepared.set(r);
    return r==null?null:r.requestId();
   }
   // An empty preparation causes the production relay to ask for claim; no second Redis call.
   if(i.getMethod().getName().equals("claimPendingReservation"))return null;
   if(i.getMethod().getName().equals("findReservation") && prepared.get()!=null){
    CouponReservation r=prepared.get();prepared.remove();
    if(!r.requestId().equals(i.getArguments()[0]))throw new IllegalStateException("Prepared ID mismatch");
    return r;
   }
   return i.proceed();
  });return (CouponIssueRedisService)proxy.getProxy();
 }
}
