package com.secrethq.store.util

import android.app.Activity
import android.app.Application
import android.os.Handler
import android.os.Looper
import android.util.Log
import androidx.lifecycle.LifecycleObserver
import com.android.billingclient.api.AcknowledgePurchaseParams
import com.android.billingclient.api.BillingClient
import com.android.billingclient.api.BillingClientStateListener
import com.android.billingclient.api.BillingFlowParams
import com.android.billingclient.api.BillingResult
import com.android.billingclient.api.ConsumeParams
import com.android.billingclient.api.PendingPurchasesParams
import com.android.billingclient.api.ProductDetails
import com.android.billingclient.api.Purchase
import com.android.billingclient.api.PurchasesUpdatedListener
import com.android.billingclient.api.QueryProductDetailsParams
import com.android.billingclient.api.QueryProductDetailsResult
import com.android.billingclient.api.QueryPurchasesParams
import com.secrethq.utils.PTServicesBridge
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import java.util.HashMap
import kotlin.math.min

private const val RECONNECT_TIMER_START_MILLISECONDS = 1L * 1000L
private const val RECONNECT_TIMER_MAX_TIME_MILLISECONDS = 1000L * 60L * 15L // 15 minutes

/**
 * Google Play Billing Library 8 implementation.
 *
 * PBL 8 removed the deprecated SkuDetails/querySkuDetailsAsync, the parameterless
 * enablePendingPurchases() and the purchase history APIs, so this class uses
 * ProductDetails, queryProductDetailsAsync and queryPurchasesAsync.
 */
class BillingDataSource(
    application: Application,
    private val defaultScope: CoroutineScope,

    ) : LifecycleObserver, PurchasesUpdatedListener, BillingClientStateListener {

    private val billingClient: BillingClient

    private val iapInfo: ArrayList<HashMap<String, String>>? by lazy {
        PTServicesBridge.getInAppIds()
    }

    // Product details for the product id currently being purchased
    private var productDetailsList: List<ProductDetails>? = null

    // how long before the data source tries to reconnect to Google play
    private var reconnectMilliseconds = RECONNECT_TIMER_START_MILLISECONDS

    val billingFlowInProcess = MutableStateFlow(false)

    val BILLING_RESPONSE_RESULT_OK = 0
    val BILLING_RESPONSE_RESULT_USER_CANCELED = 1
    val BILLING_RESPONSE_RESULT_BILLING_UNAVAILABLE = 3
    val BILLING_RESPONSE_RESULT_ITEM_UNAVAILABLE = 4
    val BILLING_RESPONSE_RESULT_DEVELOPER_ERROR = 5
    val BILLING_RESPONSE_RESULT_ERROR = 6
    val BILLING_RESPONSE_RESULT_ITEM_ALREADY_OWNED = 7
    val BILLING_RESPONSE_RESULT_ITEM_NOT_OWNED = 8
    val BILLING_RESPONSE_RESULT_NO_RESTORE = 9
    val BILLING_RESPONSE_RESULT_RESTORE_COMPLETED = 10

    private var activity: Activity? = null
    private var activeSKU: String? = null
    private var isConsumable: Boolean = false
    private var callback: ((Int, String) -> Void?)? = null
    private var restoreCallback: ((Int, String) -> Void?)? = null
    private var pendingCallback: ((Int, String) -> Void?)? = null

    private val purchaseConsumptionInProcess: MutableSet<Purchase> = HashSet()

//// ---------

    init {
        val pendingPurchasesParams = PendingPurchasesParams.newBuilder()
            .enableOneTimeProducts()
            .build()
        billingClient = BillingClient.newBuilder(application)
            .setListener(this)
            .enablePendingPurchases(pendingPurchasesParams)
            .enableAutoServiceReconnection()
            .build()
        billingClient.startConnection(this)
    }

    override fun onBillingSetupFinished(billingResult: BillingResult) {
        val responseCode = billingResult.responseCode
        val debugMessage = billingResult.debugMessage
        Log.d(TAG, "PT: onBillingSetupFinished: $responseCode $debugMessage")

        when (responseCode) {
            BillingClient.BillingResponseCode.OK -> {
                reconnectMilliseconds = RECONNECT_TIMER_START_MILLISECONDS
            }
            else -> {
                Log.d(TAG, "PT: onBillingSetupFailed. Retrying...")
                retryBillingServiceConnectionWithExponentialBackoff()
            }
        }
    }

    override fun onBillingServiceDisconnected() {
        retryBillingServiceConnectionWithExponentialBackoff()
    }

    private fun retryBillingServiceConnectionWithExponentialBackoff() {
        handler.postDelayed(
            { billingClient.startConnection(this@BillingDataSource) },
            reconnectMilliseconds
        )
        reconnectMilliseconds = min(
            reconnectMilliseconds * 2,
            RECONNECT_TIMER_MAX_TIME_MILLISECONDS
        )
    }

    fun getSkuTitle(sku: String): String? {
        return getProductDetails(sku)?.title
    }

    fun getSkuPrice(sku: String): String? {
        return getProductDetails(sku)?.let { details ->
            details.oneTimePurchaseOfferDetailsList?.firstOrNull()?.formattedPrice
                ?: details.oneTimePurchaseOfferDetails?.formattedPrice
        }
    }

    fun getSkuDescription(sku: String): String? {
        return getProductDetails(sku)?.description
    }

    private fun getProductDetails(sku: String): ProductDetails? {
        productDetailsList?.let { list ->
            for (details in list) {
                if (sku == details.productId) {
                    return details
                }
            }
        }
        return null
    }

    /**
     * Queries the product details required to start a purchase flow.
     */
    private fun queryProductDetails(sku: String) {
        if (!billingClient.isReady) {
            notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Billing service is not connected. Try again later.")
            return
        }
        val product = QueryProductDetailsParams.Product.newBuilder()
            .setProductId(sku)
            .setProductType(BillingClient.ProductType.INAPP)
            .build()
        val params = QueryProductDetailsParams.newBuilder()
            .setProductList(listOf(product))
            .build()
        billingClient.queryProductDetailsAsync(params) { billingResult, result ->
            onProductDetailsResponse(billingResult, result)
        }
    }

    private fun onProductDetailsResponse(
        billingResult: BillingResult,
        result: QueryProductDetailsResult,
    ) {
        val responseCode = billingResult.responseCode
        val debugMessage = billingResult.debugMessage
        if (responseCode != BillingClient.BillingResponseCode.OK) {
            Log.e(TAG, "onProductDetailsResponse: $responseCode $debugMessage")
            notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "onProductDetailsResponse: $responseCode $debugMessage")
            return
        }
        val products = result.productDetailsList
        if (products.isNullOrEmpty()) {
            Log.e(
                TAG,
                "onProductDetailsResponse: Found null or empty ProductDetails. " +
                        "Check to see if the products you requested are correctly published " +
                        "in the Google Play Console."
            )
            notifyPurchase(
                BILLING_RESPONSE_RESULT_ITEM_UNAVAILABLE,
                "This product is not available right now. Try again later."
            )
            return
        }
        productDetailsList = products
        processProductDetails()
    }

    /**
     * If the purchase token is not acknowledged within 3 days Google Play refunds and
     * revokes the purchase, so every purchased product is acknowledged (or consumed).
     */
    private fun processRestoredPurchasesList(purchases: List<Purchase>) {
        if (iapInfo?.isEmpty() != false) {
            Log.d(TAG, "No saved In-app info. Can't restore.")
            invokeRestore(BILLING_RESPONSE_RESULT_NO_RESTORE, "No saved In-app info. Skipping restore.")
            return
        }
        var restored = 0
        for (p in purchases) {
            if (p.purchaseState != Purchase.PurchaseState.PURCHASED) {
                continue
            }
            for (productId in p.products) {
                if (isValidIAP(productId) && !isConsumableIAP(productId)) {
                    invokeRestore(BILLING_RESPONSE_RESULT_OK, productId)
                    restored++
                }
            }
        }
        if (restored == 0) {
            invokeRestore(BILLING_RESPONSE_RESULT_NO_RESTORE, "No purchase history found. Skipping Restore")
            return
        }
        invokeRestore(BILLING_RESPONSE_RESULT_RESTORE_COMPLETED, "All products have been restored")
    }

    private fun isValidIAP(sku: String): Boolean {
        iapInfo?.let {
            for (obj in it) {
                if (obj["id"] == sku) {
                    return true
                }
            }
        }
        return false
    }

    private fun isConsumableIAP(sku: String): Boolean {
        iapInfo?.let {
            for (obj in it) {
                if (obj["id"] == sku) {
                    return obj["type"] == "consumable"
                }
            }
        }
        return false
    }

    private fun processPurchaseList(purchases: List<Purchase>?) {
        if (purchases.isNullOrEmpty()) {
            Log.d(TAG, "Empty purchase list.")
            notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Empty purchase list.")
            return
        }
        for (purchase in purchases) {
            when (purchase.purchaseState) {
                Purchase.PurchaseState.PURCHASED -> {
                    if (!isSignatureValid(purchase)) {
                        Log.e(TAG, "Invalid signature. Check to make sure your public key is correct.")
                        continue
                    }
                    val consumable = isConsumable || purchase.products.any { isConsumableIAP(it) }
                    if (consumable) {
                        consumePurchase(purchase)
                    } else {
                        processNonConsumablePurchase(purchase)
                    }
                }
                Purchase.PurchaseState.PENDING -> {
                    Log.i(TAG, "Purchase is pending approval: ${purchase.products}")
                    notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Your purchase is pending approval.")
                }
                else -> Log.w(TAG, "Unknown purchase state for ${purchase.products}")
            }
        }
    }

    private fun processNonConsumablePurchase(purchase: Purchase, wasPending: Boolean = false) {
        billingClient.acknowledgePurchase(
            AcknowledgePurchaseParams.newBuilder()
                .setPurchaseToken(purchase.purchaseToken)
                .build()
        ) { billingResult ->
            if (billingResult.responseCode != BillingClient.BillingResponseCode.OK) {
                Log.e(TAG, "Error acknowledging purchase: ${purchase.products}")
                if (wasPending) {
                    invokePending(BILLING_RESPONSE_RESULT_ERROR, "Error acknowledging purchase: ${purchase.products}")
                } else {
                    notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Error acknowledging purchase: ${purchase.products}")
                }
            } else {
                if (wasPending) {
                    invokePending(BILLING_RESPONSE_RESULT_OK, "Purchase successful.")
                } else {
                    notifyPurchase(BILLING_RESPONSE_RESULT_OK, "Purchase successful.")
                }
            }
        }
    }

    /**
     * Internal call only. Assumes that all signature checks have been completed and the purchase
     * is ready to be consumed. If the sku is already being consumed, does nothing.
     */
    private fun consumePurchase(purchase: Purchase, wasPending: Boolean = false) {
        if (purchaseConsumptionInProcess.contains(purchase)) {
            if (wasPending) {
                invokePending(BILLING_RESPONSE_RESULT_ERROR, "Already consuming the product.")
                return
            }
            notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Already consuming the product.")
            return
        }
        purchaseConsumptionInProcess.add(purchase)
        billingClient.consumeAsync(
            ConsumeParams.newBuilder()
                .setPurchaseToken(purchase.purchaseToken)
                .build()
        ) { billingResult, _ ->
            purchaseConsumptionInProcess.remove(purchase)
            if (billingResult.responseCode == BillingClient.BillingResponseCode.OK) {
                Log.d(TAG, "Consumption successful. Emitting sku.")
                if (wasPending) {
                    invokePending(BILLING_RESPONSE_RESULT_OK, "Purchase successful.")
                } else {
                    notifyPurchase(BILLING_RESPONSE_RESULT_OK, "Purchase successful.")
                }
            } else {
                Log.e(TAG, "Error while consuming: ${billingResult.debugMessage}")
                if (wasPending) {
                    invokePending(BILLING_RESPONSE_RESULT_ERROR, "Purchase was not successful.")
                } else {
                    notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Purchase was not successful.")
                }
            }
        }
    }

    fun launchBillingFlow(activity: Activity?, sku: String, isConsumable: Boolean = false, callback: (Int, String) -> Void?) {
        this.callback = callback
        this.activity = activity
        this.activeSKU = sku
        this.isConsumable = isConsumable

        queryProductDetails(sku)
    }

    fun acknowledgePendingPurchases(activity: Activity?, callback: (Int, String) -> Void?) {
        this.activity = activity
        this.pendingCallback = callback
        if (!billingClient.isReady) {
            invokePending(BILLING_RESPONSE_RESULT_ERROR, "Billing service is not connected.")
            return
        }
        val params = QueryPurchasesParams.newBuilder()
            .setProductType(BillingClient.ProductType.INAPP)
            .build()
        billingClient.queryPurchasesAsync(params) { billingResult, purchases ->
            if (billingResult.responseCode != BillingClient.BillingResponseCode.OK) {
                invokePending(BILLING_RESPONSE_RESULT_ERROR, "Unable to query purchases: ${billingResult.debugMessage}")
                return@queryPurchasesAsync
            }
            var found = false
            for (p in purchases) {
                if (p.isAcknowledged || p.purchaseState != Purchase.PurchaseState.PURCHASED) {
                    continue
                }
                found = true
                if (p.products.any { isConsumableIAP(it) }) {
                    consumePurchase(p, true)
                } else {
                    processNonConsumablePurchase(p, true)
                }
            }
            if (!found) {
                invokePending(BILLING_RESPONSE_RESULT_RESTORE_COMPLETED, "No pending purchases to acknowledge.")
            }
        }
    }

    fun restorePreviousIAPs(activity: Activity?, callback: (Int, String) -> Void?) {
        this.activity = activity
        this.restoreCallback = callback
        if (!billingClient.isReady) {
            invokeRestore(BILLING_RESPONSE_RESULT_ERROR, "Billing service is not connected.")
            return
        }
        // queryPurchaseHistoryAsync was removed in Play Billing 8. Owned, non-consumed
        // products are returned by queryPurchasesAsync, which is what restore needs.
        val params = QueryPurchasesParams.newBuilder()
            .setProductType(BillingClient.ProductType.INAPP)
            .build()
        billingClient.queryPurchasesAsync(params) { billingResult, purchases ->
            if (billingResult.responseCode != BillingClient.BillingResponseCode.OK) {
                invokeRestore(BILLING_RESPONSE_RESULT_ERROR, "Unable to restore purchases: ${billingResult.debugMessage}")
                return@queryPurchasesAsync
            }
            processRestoredPurchasesList(purchases)
        }
    }

    private fun processProductDetails() {
        val details = activeSKU?.let { getProductDetails(it) }
        if (details == null) {
            Log.e(TAG, "ProductDetails not found for: $activeSKU")
            notifyPurchase(BILLING_RESPONSE_RESULT_ITEM_UNAVAILABLE, "This product is not available right now.")
            return
        }
        val currentActivity = activity
        if (currentActivity == null) {
            notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "No activity available to start the purchase.")
            return
        }

        val productDetailsParamsBuilder = BillingFlowParams.ProductDetailsParams.newBuilder()
            .setProductDetails(details)
        val offerToken = details.oneTimePurchaseOfferDetailsList?.firstOrNull()?.offerToken
            ?: details.oneTimePurchaseOfferDetails?.offerToken
        if (!offerToken.isNullOrEmpty()) {
            productDetailsParamsBuilder.setOfferToken(offerToken)
        }

        val flowParams = BillingFlowParams.newBuilder()
            .setProductDetailsParamsList(listOf(productDetailsParamsBuilder.build()))
            .build()

        val billingResult = billingClient.launchBillingFlow(currentActivity, flowParams)
        if (billingResult.responseCode == BillingClient.BillingResponseCode.OK) {
            billingFlowInProcess.value = true
        } else {
            Log.e(TAG, "Billing failed: " + billingResult.debugMessage)
            notifyPurchase(
                BILLING_RESPONSE_RESULT_ERROR,
                "BillingResult [${billingResult.responseCode}]: ${billingResult.debugMessage}"
            )
        }
    }

    /**
     * Called by the BillingLibrary when new purchases are detected; typically in response to a
     * launchBillingFlow.
     */
    override fun onPurchasesUpdated(billingResult: BillingResult, list: List<Purchase>?) {
        when (billingResult.responseCode) {
            BillingClient.BillingResponseCode.OK -> {
                if (null != list) {
                    processPurchaseList(list)
                    billingFlowInProcess.value = false
                    return
                } else {
                    notifyPurchase(BILLING_RESPONSE_RESULT_ERROR, "Null Purchase List Returned from OK response!")
                    Log.d(TAG, "Null Purchase List Returned from OK response!")
                }
            }
            BillingClient.BillingResponseCode.USER_CANCELED -> {
                notifyPurchase(BILLING_RESPONSE_RESULT_USER_CANCELED, "User has cancelled the purchase.")
                Log.i(TAG, "onPurchasesUpdated: User canceled the purchase")
            }
            BillingClient.BillingResponseCode.ITEM_ALREADY_OWNED -> {
                Log.i(TAG, "onPurchasesUpdated: The user already owns this item")
                if (isConsumable) {
                    if (list != null) {
                        for (purchase in list) {
                            consumePurchase(purchase)
                        }
                        billingFlowInProcess.value = false
                        return
                    }
                } else {
                    notifyPurchase(BILLING_RESPONSE_RESULT_OK, "Already owned. Restore")
                }
            }
            BillingClient.BillingResponseCode.DEVELOPER_ERROR -> {
                notifyPurchase(
                    BILLING_RESPONSE_RESULT_DEVELOPER_ERROR,
                    "onPurchasesUpdated: Developer error means that Google Play " +
                            "does not recognize the configuration. If you are just getting started, " +
                            "make sure you have configured the application correctly in the " +
                            "Google Play Console. The SKU product ID must match and the APK you " +
                            "are using must be signed with release keys."
                )
                Log.e(TAG, "onPurchasesUpdated: Developer error. Check the Play Console configuration and signing key.")
            }
            else -> {
                notifyPurchase(
                    BILLING_RESPONSE_RESULT_ERROR,
                    "BillingResult [${billingResult.responseCode}]: ${billingResult.debugMessage}"
                )
                Log.d(TAG, "BillingResult [${billingResult.responseCode}]: ${billingResult.debugMessage}")
            }
        }

        billingFlowInProcess.value = false
    }

    private fun isSignatureValid(purchase: Purchase): Boolean {
        return true //Security.verifyPurchase(purchase.originalJson, purchase.signature)
    }

    private fun notifyPurchase(resultCode: Int, message: String) {
        val current = callback
        if (current != null) {
            current.invoke(resultCode, message)
        } else {
            Log.w(TAG, "No purchase callback registered: $resultCode $message")
        }
    }

    private fun invokePending(resultCode: Int, message: String) {
        pendingCallback?.invoke(resultCode, message)
    }

    private fun invokeRestore(resultCode: Int, message: String) {
        restoreCallback?.invoke(resultCode, message)
    }

    companion object {
        private val TAG = "BB3" + BillingDataSource::class.java.simpleName

        @Volatile
        var sInstance: BillingDataSource? = null
        private val handler = Handler(Looper.getMainLooper())

        @JvmStatic
        fun initialize(
            application: Application,
            defaultScope: CoroutineScope,
        ) = sInstance ?: synchronized(this) {
            sInstance ?: BillingDataSource(
                application,
                defaultScope
            ).also { sInstance = it }
        }
    }
}
